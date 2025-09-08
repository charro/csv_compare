use clap::Parser;
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use polars::frame::DataFrame;
use polars::prelude::{
    col, IndexOfSchema, IntoVec, LazyCsvReader, LazyFileListReader, LazyFrame, SortOptions,
};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::exit;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// First file to compare
    file1: String,

    /// Second file to compare
    file2: String,

    /// Whether files are required to have the columns in the same order (default: allow unordered)
    #[arg(default_value = "false", long, short)]
    strict_column_order: bool,

    /// How many columns to compare at the same time.
    /// The bigger the number the faster, but will also increase the memory consumption
    #[arg(default_value = "1", long, short)]
    number_of_columns: usize,

    /// Column separator character
    #[arg(default_value = ",", long, short = 'p')]
    separator: char,

    /// Column separator character. If not provided, first column would be used instead
    #[arg(default_value = "", long, short = 'l')]
    sorting_column: String,

    /// Generate a detailed report file when files are different
    #[arg(default_value = "false", long, short = 'r')]
    report: bool
}

fn main() {
    let args = Args::parse();

    let first_file_path = args.file1.as_str();
    let second_file_path = args.file2.as_str();
    let sorting_column = args.sorting_column.as_str();

    println!(
        "Comparing file {} with file {}. {} column(s) at a time... {}",
        first_file_path,
        second_file_path,
        args.number_of_columns,
        if args.strict_column_order {
            " Strict order of columns enforced".yellow()
        } else {
            "".white()
        }
    );

    let separator = args.separator;
    let first_file_lf = get_lazy_frame(first_file_path, separator);
    let second_file_lf = get_lazy_frame(second_file_path, separator);

    let row_num = assert_both_frames_have_same_row_num(&first_file_lf, &second_file_lf, &args, first_file_path, second_file_path);
    println!("{}: {}", "Files have same number of rows".green(), row_num);

    let first_file_cols = get_column_names(&first_file_lf);
    let second_file_cols = get_column_names(&second_file_lf);

    assert_both_frames_are_comparable(
        &first_file_cols,
        &second_file_cols,
        args.strict_column_order,
        &args,
        first_file_path,
        second_file_path,
    );
    println!("{}", "Files have comparable columns".green());

    let sorting_column = if sorting_column != "" {&sorting_column.to_string()} else {&first_file_cols[0]};
    println!("Sorting data by column {}", sorting_column);

    let columns_to_iterate = (first_file_cols.len() - 1) as u64;

    println!(
        "Comparing content of columns in both files when sorted by column \"{}\"...",
        sorting_column
    );
    let progress_bar = ProgressBar::new(columns_to_iterate);
    progress_bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .expect("Error creating progress bar. Incorrect Style?. Please raise issue to developers of this tool"));

    let number_of_columns_to_compare = args.number_of_columns;
    let mut columns_to_compare = vec![];
    for i in 1..first_file_cols.len() {
        let column_name = &first_file_cols[i];
        columns_to_compare.push(column_name);

        if columns_to_compare.len() == number_of_columns_to_compare
            || i == first_file_cols.len() - 1
        {
            let first_data_frame = get_sorted_data_frame_for_columns(
                &first_file_lf,
                sorting_column,
                &columns_to_compare,
            );

            let second_data_frame = get_sorted_data_frame_for_columns(
                &second_file_lf,
                sorting_column,
                &columns_to_compare,
            );

            if !first_data_frame.equals_missing(&second_data_frame) {
                let column_names = columns_to_compare
                    .iter()
                    .copied()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(" | ");

                println!(
                    "{}: {} \n {} \n {}",
                    "FILES ARE DIFFERENT".red(),
                    "Values for column(s)".red(),
                    column_names.red().bold(),
                    "are different".red()
                );

                // Generate report if requested
                if args.report {
                    println!("{}", "Generating detailed report of differences...".yellow());
                    if let Ok(report_file) = find_differences_and_generate_report(
                        first_file_path,
                        second_file_path,
                        separator,
                        sorting_column,
                        Some(args.number_of_columns * 1000), // Use a reasonable batch size
                    ) {
                        println!("Report saved to: {}", report_file.green());
                    } else {
                        println!("{}", "Failed to generate report file".red());
                    }
                }

                exit(3);
            }
            progress_bar.inc(columns_to_compare.len() as u64);
            columns_to_compare.clear();
        }
    }
    progress_bar.finish();

    println!(
        "Files {} and {} {} {}",
        first_file_path.bold(),
        second_file_path.bold(),
        "ARE IDENTICAL WHEN SORTED BY COLUMN:".green(),
        sorting_column.green()
    );
}

fn assert_both_frames_have_same_row_num(
    first_lazy_frame: &LazyFrame,
    second_lazy_frame: &LazyFrame,
    args: &Args,
    _file1_path: &str,
    _file2_path: &str,
) -> u32 {
    let first_row_num = get_rows_num(first_lazy_frame);
    let second_row_num = get_rows_num(second_lazy_frame);

    if first_row_num != second_row_num {
        println!(
            "{}: {} {} <> {}",
            "FILES ARE DIFFERENT".red(),
            "Different number of rows".red(),
            first_row_num.to_string(),
            second_row_num.to_string()
        );

        // Generate report if requested
        if args.report {
            println!("{}", "Cannot generate detailed report: files have different row counts".yellow());
            println!("{}", "Use files with the same number of rows for detailed comparison reports".yellow());
        }

        exit(4);
    }

    return first_row_num;
}

fn assert_both_frames_are_comparable(
    first_file_cols: &[String],
    second_file_cols: &[String],
    is_strict_order: bool,
    args: &Args,
    file1_path: &str,
    file2_path: &str,
) {
    let have_same_columns = if is_strict_order {
        first_file_cols.eq(second_file_cols)
    } else {
        // Convert the vectors into sets to ignore the order
        let set1: HashSet<_> = first_file_cols.iter().collect();
        let set2: HashSet<_> = second_file_cols.iter().collect();
        set1 == set2
    };

    if !have_same_columns {
        println!(
            "{}: {} => [{}] != [{}]",
            "FILES ARE DIFFERENT".red(),
            "Different columns".red(),
            first_file_cols.join(",").bold().yellow(),
            second_file_cols.join(",").bold().blue()
        );
        if is_strict_order {
            println!(
                "{} {} {}",
                "Hint:",
                "--strict-order".bold(),
                "flag is active"
            );
        }

        // Generate report if requested
        if args.report {
            println!("{}", "Generating detailed report of differences...".yellow());
            // Get the first column name from the first file for sorting
            let first_col_name = first_file_cols[0].clone();
            if let Ok(report_file) = find_differences_and_generate_report(
                file1_path,
                file2_path,
                args.separator,
                &first_col_name,
                Some(20000), // Default batch size
            ) {
                println!("Report saved to: {}", report_file.green());
            } else {
                println!("{}", "Failed to generate report file".red());
            }
        }

        exit(2);
    }
}

fn get_lazy_frame(file_path: &str, delimiter: char) -> LazyFrame {
    LazyCsvReader::new(file_path)
        .has_header(true)
        .with_infer_schema_length(Some(0))
        .with_separator(delimiter as u8)
        .finish()
        .expect(format!("Couldn't open file {file_path}").as_str())
}

fn get_column_names(lazy_frame: &LazyFrame) -> Vec<String> {
    let schema = lazy_frame
        .clone()
        .limit(1)
        .collect()
        .expect("Couldn't parse first CSV file")
        .schema();

    schema.get_names().into_vec()
}

fn get_sorted_data_frame_for_columns(
    lazy_frame: &LazyFrame,
    sorting_by_column: &String,
    columns: &Vec<&String>,
) -> DataFrame {
    let mut all_columns = vec![col(sorting_by_column)];
    for next_column in columns {
        // We must de-reference next_column as the iterator returns a reference of a reference
        if *next_column != sorting_by_column {
            all_columns.push(col(next_column));
        }
    }

    lazy_frame
        .clone()
        .select(all_columns)
        .sort(sorting_by_column, SortOptions::default())
        .collect()
        .expect(format!("Couldn't sort by column {sorting_by_column}",).as_str())
}

fn get_rows_num(lazy_frame: &LazyFrame) -> u32 {
    let first_column_name = get_column_names(&lazy_frame.clone())[0].to_string();
    return lazy_frame
        .clone()
        .select([col(first_column_name.as_str())])
        .collect()
        .expect("Error when counting the rows of the CSV file")
        .shape()
        .0 as u32;
}

/// Finds differences between two CSV files and generates a detailed report
/// Assumes both files have the same column names and same number of rows
/// Processes files in batches to handle large files efficiently
fn find_differences_and_generate_report(
    file1_path: &str,
    file2_path: &str,
    separator: char,
    sorting_column: &str,
    batch_size: Option<usize>,
) -> Result<String, Box<dyn std::error::Error>> {
    let batch_size = batch_size.unwrap_or(20000); // Default batch size of 20k rows

    // Create lazy frames for both files
    let lf1 = LazyCsvReader::new(file1_path)
        .has_header(true)
        .with_infer_schema_length(Some(0))
        .with_separator(separator as u8)
        .finish()?
        .sort(sorting_column, SortOptions::default());

    let lf2 = LazyCsvReader::new(file2_path)
        .has_header(true)
        .with_infer_schema_length(Some(0))
        .with_separator(separator as u8)
        .finish()?
        .sort(sorting_column, SortOptions::default());

    // Get total row count and column names from first batch
    let first_batch1 = lf1.clone().limit(batch_size as u32).collect()?;
    let total_rows = lf1.clone().select([col(sorting_column)]).collect()?.height();
    let column_names = first_batch1.get_column_names();

    // Generate report filename using file names
    let file1_name = Path::new(file1_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file1");
    let file2_name = Path::new(file2_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file2");

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs();
    let report_filename = format!("csv_diff_{}_vs_{}_{}.txt", file1_name, file2_name, timestamp);
    let csv_filename = format!("csv_diff_{}_vs_{}_{}.csv", file1_name, file2_name, timestamp);

    // Create report file
    let mut report_file = File::create(&report_filename)?;

    // Write report header
    writeln!(report_file, "CSV Differences Report")?;
    writeln!(report_file, "======================")?;
    writeln!(report_file, "File 1: {}", file1_path)?;
    writeln!(report_file, "File 2: {}", file2_path)?;
    writeln!(report_file, "Sorting Column: {}", sorting_column)?;
    writeln!(report_file, "Total Rows: {}", total_rows)?;
    writeln!(report_file, "Total Columns: {}", column_names.len())?;
    writeln!(report_file, "Batch Size: {}", batch_size)?;
    writeln!(report_file, "")?;

    // Collect all differences first
    let mut all_differences = Vec::new();

    let mut total_differences = 0;
    let mut rows_with_differences = 0;
    let mut global_row_idx = 0;

    // Create progress bar for large files
    let progress_bar = if total_rows > 100000 {
        let pb = ProgressBar::new(total_rows as u64);
        pb.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                .expect("Error creating progress bar")
        );
        pb.set_message("Comparing CSV files...");
        Some(pb)
    } else {
        None
    };

    // Process files in batches
    for batch_start in (0..total_rows).step_by(batch_size) {
        let batch_end = std::cmp::min(batch_start + batch_size, total_rows);
        let current_batch_size = batch_end - batch_start;

        // Load current batch from both files
        let df1_batch = lf1.clone()
            .slice(batch_start as i64, current_batch_size as u32)
            .collect()?;

        let df2_batch = lf2.clone()
            .slice(batch_start as i64, current_batch_size as u32)
            .collect()?;

        // Compare each row in the current batch
        for batch_row_idx in 0..current_batch_size {
            let mut row_differences = Vec::new();

            // Get the sorting column value for this row (used for identification)
            let sorting_col1 = df1_batch.column(sorting_column).unwrap();
            let sorting_value = sorting_col1.get(batch_row_idx).unwrap().to_string();

            // Compare each column in the current row
            for col_name in &column_names {
                // Get values from both dataframes using column access
                let col1 = df1_batch.column(col_name).unwrap();
                let col2 = df2_batch.column(col_name).unwrap();

                let val1 = col1.get(batch_row_idx).unwrap();
                let val2 = col2.get(batch_row_idx).unwrap();

                // Check if values are different (already strings due to infer_schema_length=0)
                if val1 != val2 {
                    row_differences.push((col_name.to_string(), val1.to_string(), val2.to_string()));
                    total_differences += 1;
                }
            }

            // If this row has differences, collect them
            if !row_differences.is_empty() {
                rows_with_differences += 1;
                all_differences.push((global_row_idx + 1, sorting_value, row_differences));
            }

            global_row_idx += 1;

            // Update progress bar if it exists
            if let Some(ref pb) = progress_bar {
                pb.inc(1);
            }
        }
    }

    // Finish progress bar if it exists
    if let Some(pb) = progress_bar {
        pb.finish();
    }

    // Decide on output format and create files accordingly
    let mut csv_file_created = false;

    if total_differences == 0 {
        writeln!(report_file, "✅ Files are identical!")?;
    } else if total_differences <= 50 {
        // Use detailed format for small number of differences
        writeln!(report_file, "Differences found:")?;
        writeln!(report_file, "==================")?;
        writeln!(report_file, "")?;

        for (row_num, sorting_value, row_diffs) in &all_differences {
            writeln!(report_file, "Row {} ({} = {}):", row_num, sorting_column, sorting_value)?;
            for (col_name, val1, val2) in row_diffs {
                writeln!(report_file, "  Column '{}':", col_name)?;
                writeln!(report_file, "    File 1: {}", val1)?;
                writeln!(report_file, "    File 2: {}", val2)?;
            }
            writeln!(report_file, "")?;
        }
    } else {
        // Create separate CSV file for many differences
        writeln!(report_file, "Many differences found ({} total differences).", total_differences)?;
        writeln!(report_file, "Creating separate CSV file for detailed analysis: {}", csv_filename)?;
        writeln!(report_file, "========================================================")?;
        writeln!(report_file, "")?;

        // Create CSV file
        let mut csv_file = File::create(&csv_filename)?;
        csv_file_created = true;

        // Write CSV header
        writeln!(csv_file, "Row,Sorting_Value,Column,File_1_Value,File_2_Value")?;

        // Helper function to escape CSV values properly
        let escape_csv = |s: &str| -> String {
            // Remove surrounding quotes if they exist (from polars string conversion)
            let cleaned = if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
                &s[1..s.len()-1]
            } else {
                s
            };

            if cleaned.contains(',') || cleaned.contains('"') || cleaned.contains('\n') || cleaned.contains('\r') {
                format!("\"{}\"", cleaned.replace("\"", "\"\""))
            } else {
                cleaned.to_string()
            }
        };

        // Write all differences to CSV
        for (row_num, sorting_value, row_diffs) in &all_differences {
            for (col_name, val1, val2) in row_diffs {
                writeln!(csv_file, "{},{},{},{},{}",
                    row_num,
                    escape_csv(sorting_value),
                    escape_csv(col_name),
                    escape_csv(val1),
                    escape_csv(val2)
                )?;
            }
        }

        // Show a sample of first few differences in the text report
        writeln!(report_file, "Sample of first 10 differences (see {} for complete list):", csv_filename)?;
        writeln!(report_file, "=================================================================")?;
        writeln!(report_file, "")?;

        let sample_count = std::cmp::min(10, all_differences.len());
        for i in 0..sample_count {
            let (row_num, sorting_value, row_diffs) = &all_differences[i];
            writeln!(report_file, "Row {} ({} = {}):", row_num, sorting_column, sorting_value)?;
            for (col_name, val1, val2) in row_diffs {
                writeln!(report_file, "  Column '{}':", col_name)?;
                writeln!(report_file, "    File 1: {}", val1)?;
                writeln!(report_file, "    File 2: {}", val2)?;
            }
            writeln!(report_file, "")?;
        }

        if all_differences.len() > sample_count {
            writeln!(report_file, "... and {} more rows with differences (see CSV file for complete details)",
                all_differences.len() - sample_count)?;
            writeln!(report_file, "")?;
        }
    }

    // Write summary
    writeln!(report_file, "Summary:")?;
    writeln!(report_file, "=========")?;
    writeln!(report_file, "Total rows with differences: {}", rows_with_differences)?;
    writeln!(report_file, "Total individual cell differences: {}", total_differences)?;
    writeln!(report_file, "Rows without differences: {}", total_rows - rows_with_differences)?;

    if csv_file_created {
        writeln!(report_file, "Detailed differences exported to: {}", csv_filename)?;
    }

    if total_differences > 0 {
        writeln!(report_file, "")?;
        if total_differences <= 50 {
            writeln!(report_file, "❌ Files have differences as listed above.")?;
        } else {
            writeln!(report_file, "❌ Files have many differences. See {} for detailed analysis in Excel.", csv_filename)?;
        }
    }

    // Return the main report filename (caller can infer CSV filename if needed)
    Ok(report_filename)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_find_differences_and_generate_report() {
        // Create two simple test CSV files
        let csv1_content = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35";
        let csv2_content = "id,name,age\n1,Alice,26\n2,Bob,30\n3,Charlie,36";

        // Write test files
        fs::write("test1.csv", csv1_content).unwrap();
        fs::write("test2.csv", csv2_content).unwrap();

        // Test the function
        let result = find_differences_and_generate_report("test1.csv", "test2.csv", ',', "id", Some(1000));

        // Clean up test files
        fs::remove_file("test1.csv").unwrap_or_default();
        fs::remove_file("test2.csv").unwrap_or_default();

        // Check that the function succeeded
        assert!(result.is_ok());

        let report_file = result.unwrap();

        // Check that the report file was created
        assert!(fs::metadata(&report_file).is_ok());

        // Check that the report file was created with the expected naming pattern
        assert!(report_file.starts_with("csv_diff_test1_vs_test2_"));
        assert!(report_file.ends_with(".txt"));

        // Read and verify report content
        let report_content = fs::read_to_string(&report_file).unwrap();
        assert!(report_content.contains("CSV Differences Report"));
        assert!(report_content.contains("File 1: test1.csv"));
        assert!(report_content.contains("File 2: test2.csv"));
        assert!(report_content.contains("Row 1 (id = \"1\")"));
        assert!(report_content.contains("Row 3 (id = \"3\")"));
        assert!(report_content.contains("age"));
        assert!(report_content.contains("25"));
        assert!(report_content.contains("26"));
        assert!(report_content.contains("35"));
        assert!(report_content.contains("36"));

        // Clean up report file
        fs::remove_file(&report_file).unwrap_or_default();
    }

    #[test]
    fn test_progress_bar_for_large_files() {
        // Create larger test CSV files to trigger progress bar
        let mut csv1_content = String::from("id,name,age\n");
        let mut csv2_content = String::from("id,name,age\n");

        // Generate 150,000 rows to trigger progress bar (threshold is 100,000)
        for i in 1..=150000 {
            csv1_content.push_str(&format!("{},Alice,25\n", i));
            csv2_content.push_str(&format!("{},Alice,{}\n", i, if i % 2 == 0 { 25 } else { 26 }));
        }

        // Write test files
        fs::write("large_test1.csv", csv1_content).unwrap();
        fs::write("large_test2.csv", csv2_content).unwrap();

        // Test the function with a small batch size to see progress updates
        let result = find_differences_and_generate_report("large_test1.csv", "large_test2.csv", ',', "id", Some(1000));

        // Clean up test files
        fs::remove_file("large_test1.csv").unwrap_or_default();
        fs::remove_file("large_test2.csv").unwrap_or_default();

        // Check that the function succeeded
        assert!(result.is_ok());

        let report_file = result.unwrap();

        // Check that the report file was created
        assert!(fs::metadata(&report_file).is_ok());

        // Read and verify report content
        let report_content = fs::read_to_string(&report_file).unwrap();
        assert!(report_content.contains("CSV Differences Report"));
        assert!(report_content.contains("Total Rows: 150000"));
        assert!(report_content.contains("Batch Size: 1000"));

        // Clean up report file
        fs::remove_file(&report_file).unwrap_or_default();
    }
}


