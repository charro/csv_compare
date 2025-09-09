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

    /// Sorting column(s). If not provided, first column would be used instead.
    /// For multiple columns, separate them with commas (e.g., "id,date,type")
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

    // Parse sorting columns
    let sorting_columns: Vec<String> = if sorting_column.is_empty() {
        vec![first_file_cols[0].clone()]
    } else {
        sorting_column.split(',').map(|s| s.trim().to_string()).collect()
    };

    // Validate that all sorting columns exist
    for sort_col in &sorting_columns {
        if !first_file_cols.contains(sort_col) {
            println!("{}: Sorting column '{}' not found in files", "ERROR".red(), sort_col);
            exit(1);
        }
    }

    println!("Sorting data by column(s): {}", sorting_columns.join(", "));

    let columns_to_iterate = (first_file_cols.len() - sorting_columns.len()) as u64;

    println!(
        "Comparing content of columns in both files when sorted by column(s) \"{}\"...",
        sorting_columns.join(", ")
    );
    let progress_bar = ProgressBar::new(columns_to_iterate);
    progress_bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .expect("Error creating progress bar. Incorrect Style?. Please raise issue to developers of this tool"));

    let number_of_columns_to_compare = args.number_of_columns;
    let mut columns_to_compare = vec![];
    let mut columns_with_differences = HashSet::new(); // Track columns that have differences
    let mut files_are_different = false; // Track if any differences were found

    // Skip sorting columns when comparing
    for i in 0..first_file_cols.len() {
        let column_name = &first_file_cols[i];

        // Skip sorting columns
        if sorting_columns.contains(column_name) {
            continue;
        }

        columns_to_compare.push(column_name);

        if columns_to_compare.len() == number_of_columns_to_compare
            || i == first_file_cols.len() - 1
        {
            let first_data_frame = get_sorted_data_frame_for_columns(
                &first_file_lf,
                &sorting_columns,
                &columns_to_compare,
            );

            let second_data_frame = get_sorted_data_frame_for_columns(
                &second_file_lf,
                &sorting_columns,
                &columns_to_compare,
            );

            if !first_data_frame.equals_missing(&second_data_frame) {
                files_are_different = true;

                // Find which specific columns have differences
                for col_name in &columns_to_compare {
                    let col1 = first_data_frame.column(col_name).unwrap();
                    let col2 = second_data_frame.column(col_name).unwrap();

                    // Check if this specific column has differences
                    if col1 != col2 {
                        columns_with_differences.insert(col_name.to_string());
                    }
                }
            }
            progress_bar.inc(columns_to_compare.len() as u64);
            columns_to_compare.clear();
        }
    }
    progress_bar.finish();

    // Now check if files are different and handle accordingly
    if files_are_different {
        // Convert HashSet to sorted Vec for consistent output
        let mut diff_columns: Vec<String> = columns_with_differences.iter().cloned().collect();
        diff_columns.sort();

        println!(
            "{}: {}",
            "FILES ARE DIFFERENT".red(),
            "The following columns have differences:".red()
        );

        for col_name in &diff_columns {
            println!("  • {}", col_name.yellow().bold());
        }

        println!(
            "\n{} {} {} {}",
            "Total columns with differences:".red(),
            diff_columns.len().to_string().red().bold(),
            "out of".red(),
            (first_file_cols.len() - sorting_columns.len()).to_string().red().bold() // Exclude sorting columns
        );

        // Generate report if requested
        if args.report {
            println!("\n{}", "Generating detailed report of differences...".yellow());
            if let Ok(report_file) = find_differences_and_generate_report(
                first_file_path,
                second_file_path,
                separator,
                &sorting_columns,
                Some(args.number_of_columns * 1000), // Use a reasonable batch size
                &columns_with_differences, // Pass the columns that have differences
            ) {
                println!("Report saved to: {}", report_file.green());
            } else {
                println!("{}", "Failed to generate report file".red());
            }
        }

        exit(3);
    } else {
        println!(
            "Files {} and {} {} {}",
            first_file_path.bold(),
            second_file_path.bold(),
            "ARE IDENTICAL WHEN SORTED BY COLUMN(S):".green(),
            sorting_columns.join(", ").green()
        );
    }
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
            let first_col_names = vec![first_file_cols[0].clone()];
            // For column differences, we need to compare all columns
            let all_columns: HashSet<String> = first_file_cols.iter().skip(1).cloned().collect();
            if let Ok(report_file) = find_differences_and_generate_report(
                file1_path,
                file2_path,
                args.separator,
                &first_col_names,
                Some(20000), // Default batch size
                &all_columns, // Compare all columns when column structure is different
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
    sorting_by_columns: &Vec<String>,
    columns: &Vec<&String>,
) -> DataFrame {
    let mut all_columns = Vec::new();

    // Add sorting columns first
    for sort_col in sorting_by_columns {
        all_columns.push(col(sort_col));
    }

    // Add comparison columns
    for next_column in columns {
        // We must de-reference next_column as the iterator returns a reference of a reference
        if !sorting_by_columns.contains(*next_column) {
            all_columns.push(col(next_column));
        }
    }

    let mut lazy_query = lazy_frame.clone().select(all_columns);

    // Sort by multiple columns
    for sort_col in sorting_by_columns {
        lazy_query = lazy_query.sort(sort_col, SortOptions::default());
    }

    lazy_query
        .collect()
        .expect("Couldn't sort by the specified columns")
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
/// Uses temporary sorted files for efficient line-by-line comparison
/// Only compares columns that are known to have differences for efficiency
fn find_differences_and_generate_report(
    file1_path: &str,
    file2_path: &str,
    separator: char,
    sorting_columns: &Vec<String>,
    _batch_size: Option<usize>,
    columns_with_differences: &HashSet<String>, // Only compare these columns
) -> Result<String, Box<dyn std::error::Error>> {
    use std::io::{BufRead, BufReader};

    // Create lazy frames for both files
    let mut lf1 = LazyCsvReader::new(file1_path)
        .has_header(true)
        .with_infer_schema_length(Some(0))
        .with_separator(separator as u8)
        .finish()?;

    let mut lf2 = LazyCsvReader::new(file2_path)
        .has_header(true)
        .with_infer_schema_length(Some(0))
        .with_separator(separator as u8)
        .finish()?;

    // Sort by multiple columns
    for sort_col in sorting_columns {
        lf1 = lf1.sort(sort_col, SortOptions::default());
        lf2 = lf2.sort(sort_col, SortOptions::default());
    }

    // Get all column names to understand the structure
    let all_column_names = get_column_names(&lf1);
    let total_rows = lf1.clone().select([col(&sorting_columns[0])]).collect()?.height();

    println!("Creating temporary sorted files for efficient comparison...");

    // Create list of columns we need for comparison (sorting + difference columns)
    let mut columns_to_export = sorting_columns.clone();
    for col_name in &all_column_names {
        if columns_with_differences.contains(col_name) && !sorting_columns.contains(col_name) {
            columns_to_export.push(col_name.clone());
        }
    }

    let temp_dir = std::env::temp_dir();
    let temp_file1 = temp_dir.join(format!("csv_compare_temp1_{}.csv", std::process::id()));
    let temp_file2 = temp_dir.join(format!("csv_compare_temp2_{}.csv", std::process::id()));

    // Export sorted data to temporary files
    let columns_to_select: Vec<_> = columns_to_export.iter().map(|s| col(s)).collect();

    let df1_sorted = lf1.select(columns_to_select.clone()).collect()?;
    let df2_sorted = lf2.select(columns_to_select).collect()?;

    // Write temporary CSV files manually for better control
    write_dataframe_to_csv(&df1_sorted, &temp_file1, separator)?;
    write_dataframe_to_csv(&df2_sorted, &temp_file2, separator)?;

    println!("Temporary files created. Starting line-by-line comparison...");

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
    writeln!(report_file, "Sorting Column(s): {}", sorting_columns.join(", "))?;
    writeln!(report_file, "Total Rows: {}", total_rows)?;
    writeln!(report_file, "Total Columns: {}", all_column_names.len())?;
    writeln!(report_file, "Columns with differences: {:?}", columns_with_differences.iter().collect::<Vec<_>>())?;
    writeln!(report_file, "")?;

    // Now do fast line-by-line comparison
    let file1_reader = BufReader::new(std::fs::File::open(&temp_file1)?);
    let file2_reader = BufReader::new(std::fs::File::open(&temp_file2)?);

    let mut file1_lines = file1_reader.lines();
    let mut file2_lines = file2_reader.lines();

    // Skip headers
    let header1 = file1_lines.next().unwrap()?;
    let _header2 = file2_lines.next().unwrap()?;

    // Parse headers to know column positions
    let header1_cols: Vec<&str> = header1.split(separator).collect();

    // Find column indices for sorting columns and difference columns
    let mut sorting_indices = Vec::new();
    let mut diff_column_indices = Vec::new();
    let mut diff_column_names_ordered = Vec::new();

    for sort_col in sorting_columns {
        if let Some(pos) = header1_cols.iter().position(|&x| x == sort_col) {
            sorting_indices.push(pos);
        }
    }

    for col_name in columns_with_differences {
        if let Some(pos) = header1_cols.iter().position(|&x| x == col_name) {
            diff_column_indices.push(pos);
            diff_column_names_ordered.push(col_name.clone());
        }
    }

    // Progress tracking
    let progress_bar = if total_rows > 50000 {
        let pb = ProgressBar::new(total_rows as u64);
        pb.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg} {per_sec}")
                .expect("Error creating progress bar")
        );
        pb.set_message("Comparing lines");
        Some(pb)
    } else {
        None
    };

    let mut all_differences = Vec::new();
    let mut total_differences = 0;
    let mut rows_with_differences = 0;
    let mut line_num = 0;

    // Compare lines efficiently
    loop {
        let line1_result = file1_lines.next();
        let line2_result = file2_lines.next();

        match (line1_result, line2_result) {
            (Some(Ok(line1)), Some(Ok(line2))) => {
                line_num += 1;

                let cols1: Vec<&str> = line1.split(separator).collect();
                let cols2: Vec<&str> = line2.split(separator).collect();

                // Build composite key for identification
                let mut sorting_values = Vec::new();
                for &idx in &sorting_indices {
                    if idx < cols1.len() {
                        let col_name = &sorting_columns[sorting_indices.iter().position(|&x| x == idx).unwrap()];
                        sorting_values.push(format!("{} = {}", col_name, cols1[idx]));
                    }
                }
                let composite_key = sorting_values.join(", ");

                // Compare difference columns
                let mut row_differences = Vec::new();
                for (i, &col_idx) in diff_column_indices.iter().enumerate() {
                    if col_idx < cols1.len() && col_idx < cols2.len() {
                        let val1 = cols1[col_idx];
                        let val2 = cols2[col_idx];

                        if val1 != val2 {
                            row_differences.push((diff_column_names_ordered[i].clone(), val1.to_string(), val2.to_string()));
                            total_differences += 1;
                        }
                    }
                }

                if !row_differences.is_empty() {
                    rows_with_differences += 1;
                    all_differences.push((composite_key, row_differences));
                }

                // Update progress every 1000 lines for better performance
                if let Some(ref pb) = progress_bar {
                    if line_num % 1000 == 0 {
                        pb.inc(1000);
                    }
                }
            }
            (None, None) => break, // Both files ended
            _ => {
                // This shouldn't happen if files have same number of rows
                break;
            }
        }
    }

    // Update progress bar to completion
    if let Some(ref pb) = progress_bar {
        pb.set_position(total_rows as u64);
        pb.finish_with_message("Comparison complete");
    }

    // Clean up temporary files
    let _ = std::fs::remove_file(&temp_file1);
    let _ = std::fs::remove_file(&temp_file2);

    println!("Temporary files cleaned up. Generating report...");

    // Decide on output format and create files accordingly
    let mut csv_file_created = false;

    if total_differences == 0 {
        writeln!(report_file, "✅ Files are identical!")?;
    } else if total_differences <= 50 {
        // Use detailed format for small number of differences
        writeln!(report_file, "Differences found:")?;
        writeln!(report_file, "==================")?;
        writeln!(report_file, "")?;

        for (composite_key, row_diffs) in &all_differences {
            writeln!(report_file, "{}:", composite_key)?;
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

        // Create CSV file with buffered writing for better performance
        let csv_file = File::create(&csv_filename)?;
        let mut csv_writer = std::io::BufWriter::with_capacity(65536, csv_file);
        csv_file_created = true;

        // Write CSV header - with composite key
        writeln!(csv_writer, "Composite_Key,Column,File_1_Value,File_2_Value")?;

        // Improved CSV escaping function
        let escape_csv = |s: &str| -> String {
            // Remove any existing quotes first to avoid double-escaping
            let cleaned = s.trim_matches('"');

            // Only escape if the value contains special characters
            if cleaned.contains(',') || cleaned.contains('"') || cleaned.contains('\n') || cleaned.contains('\r') {
                format!("\"{}\"", cleaned.replace("\"", "\"\""))
            } else {
                cleaned.to_string()
            }
        };

        // Write all differences to CSV efficiently
        for (composite_key, row_diffs) in &all_differences {
            for (col_name, val1, val2) in row_diffs {
                writeln!(csv_writer, "{},{},{},{}",
                         escape_csv(composite_key),
                         escape_csv(col_name),
                         escape_csv(val1),
                         escape_csv(val2)
                )?;
            }
        }

        // Ensure all data is written
        csv_writer.flush()?;

        // Show a sample of first few differences in the text report
        writeln!(report_file, "Sample of first 10 differences (see {} for complete list):", csv_filename)?;
        writeln!(report_file, "=================================================================")?;
        writeln!(report_file, "")?;

        let sample_count = std::cmp::min(10, all_differences.len());
        for i in 0..sample_count {
            let (composite_key, row_diffs) = &all_differences[i];
            writeln!(report_file, "{}:", composite_key)?;
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

/// Helper function to write a DataFrame to CSV manually with proper escaping
fn write_dataframe_to_csv(
    df: &DataFrame,
    file_path: &Path,
    separator: char
) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::BufWriter;

    let file = std::fs::File::create(file_path)?;
    let mut writer = BufWriter::new(file);

    // Write header
    let column_names = df.get_column_names();
    writeln!(writer, "{}", column_names.join(&separator.to_string()))?;

    // Improved CSV value escaping
    let escape_csv_value = |value: String| -> String {
        // Handle null/None values from Polars
        if value == "null" {
            return String::new();
        }

        // Remove any existing outer quotes from Polars string representation
        let cleaned = value.trim_matches('"');

        // Only add quotes if necessary
        if cleaned.contains(separator) || cleaned.contains('"') || cleaned.contains('\n') || cleaned.contains('\r') {
            format!("\"{}\"", cleaned.replace("\"", "\"\""))
        } else {
            cleaned.to_string()
        }
    };

    // Write data rows
    let height = df.height();
    for row_idx in 0..height {
        let mut row_values = Vec::new();

        for col_name in &column_names {
            let col = df.column(col_name).unwrap();
            let value = col.get(row_idx).unwrap().to_string();
            let escaped_value = escape_csv_value(value);
            row_values.push(escaped_value);
        }

        writeln!(writer, "{}", row_values.join(&separator.to_string()))?;
    }

    writer.flush()?;
    Ok(())
}
