# CSV Compare Tool

A high-performance command-line tool built in Rust for comparing large CSV files efficiently. This tool can detect differences between CSV files and generate detailed reports, making it perfect for data validation, ETL pipeline testing, and data quality assurance.

## Features

- 🚀 **High Performance**: Built with Rust and Polars for lightning-fast CSV processing
- 📊 **Memory Efficient**: Configurable batch processing to handle large files without excessive memory usage
- 🔍 **Flexible Column Comparison**: Compare files with columns in different orders
- 📋 **Multi-column Sorting**: Sort by single or multiple columns for accurate row matching
- 📝 **Detailed Reports**: Generate comprehensive difference reports in both text and CSV formats
- 🎯 **Smart Detection**: Only reports columns that actually have differences
- 🔧 **Customizable**: Support for different separators and comparison strategies
- 📈 **Progress Tracking**: Real-time progress bars for long-running comparisons

## Installation

Download the latest binary from the [Releases](https://github.com/charro/csv_compare/releases) page.

### Build from Source

### Prerequisites
- Rust (latest stable version)
- Cargo package manager

```bash
git clone https://github.com/charro/csv_compare.git
cd csv_compare
cargo build --release
```

The compiled binary will be available at `target/release/csv-compare`.

## Usage

### Basic Usage
```bash
csv-compare file1.csv file2.csv
```

### Advanced Usage
```bash
csv-compare [OPTIONS] <FILE1> <FILE2>
```

## Command Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--strict-column-order` | `-s` | Require columns to be in the same order | `false` |
| `--number-of-columns` | `-n` | Number of columns to compare simultaneously | `1` |
| `--separator` | `-p` | Column separator character | `,` |
| `--sorting-column` | `-l` | Column(s) to sort by (comma-separated) | First column |
| `--report` | `-r` | Generate detailed difference report | `false` |

## Examples

### Example 1: Basic Comparison
```bash
csv-compare sales_2023.csv sales_2024.csv
```

**Output:**
```
Comparing file sales_2023.csv with file sales_2024.csv. 1 column(s) at a time...
Files have same number of rows: 10000
Files have comparable columns
Sorting data by column(s): id
Comparing content of columns in both files when sorted by column(s) "id"...
[00:00:02] ████████████████████████████████████████ 5/5
Files sales_2023.csv and sales_2024.csv ARE IDENTICAL WHEN SORTED BY COLUMN(S): id
```

### Example 2: Files with Differences
```bash
csv-compare --report data_v1.csv data_v2.csv
```

**Output:**
```
FILES ARE DIFFERENT: The following columns have differences:
  • price
  • quantity
  • total

Total columns with differences: 3 out of 5

Generating detailed report of differences...
Report saved to: csv_diff_data_v1_vs_data_v2_1672531200.txt
```

### Example 3: Custom Sorting and Processing
```bash
csv-compare \
  --sorting-column "customer_id,date,product_id" \
  --number-of-columns 5 \
  --separator ";" \
  --report \
  transactions_old.csv transactions_new.csv
```

### Example 4: Strict Column Order
```bash
csv-compare --strict-column-order schema_v1.csv schema_v2.csv
```

## How It Works

### 1. Initial Validation
- **Row Count Check**: Ensures both files have the same number of rows
- **Column Structure**: Validates that both files have compatible columns
- **Sorting Column Validation**: Confirms specified sorting columns exist in both files

### 2. Efficient Comparison Strategy
- **Batch Processing**: Processes columns in configurable batches to optimize memory usage
- **Smart Sorting**: Uses Polars' efficient sorting algorithms for consistent row ordering
- **Selective Comparison**: Only compares columns that show differences for performance

### 3. Report Generation
- **Text Reports**: Human-readable summaries with sample differences
- **CSV Reports**: Machine-readable detailed differences for further analysis
- **Smart Formatting**: Automatically chooses report format based on number of differences

## Report Formats

### Text Report (.txt)
```
CSV Differences Report
======================
File 1: sales_2023.csv
File 2: sales_2024.csv
Sorting Column(s): customer_id
Total Rows: 10000
Total Columns: 8
Columns with differences: ["price", "discount"]

Differences found:
==================

customer_id = 12345:
  Column 'price':
    File 1: 99.99
    File 2: 89.99
  Column 'discount':
    File 1: 0.10
    File 2: 0.15

Summary:
=========
Total rows with differences: 1247
Total individual cell differences: 2156
Rows without differences: 8753
```

### CSV Report (.csv)
For large numbers of differences, a separate CSV file is generated:
```csv
Composite_Key,Column,File_1_Value,File_2_Value
"customer_id = 12345",price,99.99,89.99
"customer_id = 12345",discount,0.10,0.15
"customer_id = 67890, date = 2024-01-15",quantity,5,3
```

## Performance Characteristics

### Memory Usage
- **Configurable Batching**: Control memory usage with `--number-of-columns`
- **Lazy Loading**: Uses Polars' lazy evaluation for memory efficiency
- **Streaming Processing**: Handles files larger than available RAM

### Speed Optimizations
- **Parallel Processing**: Leverages multiple CPU cores
- **Efficient Sorting**: Uses optimized sorting algorithms
- **Smart Comparison**: Only processes columns with actual differences
- **Temporary File Strategy**: Uses efficient file I/O for large datasets

## Use Cases

### 1. Data Migration Validation
```bash
# Compare source and target after ETL process
csv-compare \
  --sorting-column "primary_key" \
  --report \
  source_data.csv migrated_data.csv
```

### 2. A/B Testing Data Validation
```bash
# Compare experiment results with different processing
csv-compare \
  --sorting-column "user_id,timestamp" \
  --number-of-columns 10 \
  experiment_a.csv experiment_b.csv
```

### 3. Schema Evolution Testing
```bash
# Verify backward compatibility with strict column order
csv-compare \
  --strict-column-order \
  schema_v1.csv schema_v2.csv
```

### 4. Data Quality Monitoring
```bash
# Regular comparison of daily exports
csv-compare \
  --sorting-column "date,id" \
  --report \
  daily_export_$(date -d yesterday +%Y%m%d).csv \
  daily_export_$(date +%Y%m%d).csv
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Files are identical |
| 1 | Invalid sorting column specified |
| 2 | Files have different column structure |
| 3 | Files have content differences |
| 4 | Files have different row counts |

## Troubleshooting

### Common Issues

**Issue**: "Sorting column not found"
```bash
ERROR: Sorting column 'customer_id' not found in files
```
**Solution**: Check column names in your CSV files. Use the first few rows to verify exact column names including spaces and case sensitivity.

**Issue**: Memory usage too high
**Solution**: Reduce the `--number-of-columns` parameter:
```bash
csv-compare --number-of-columns 1 large_file1.csv large_file2.csv
```

**Issue**: Different column structures
```bash
FILES ARE DIFFERENT: Different columns => [col1,col2,col3] != [col1,col3,col2]
```
**Solution**: If column order doesn't matter, remove the `--strict-column-order` flag (it's disabled by default).

### Performance Tips

1. **Use appropriate batch sizes**: Larger `--number-of-columns` values are faster but use more memory
2. **Choose good sorting columns**: Use indexed or naturally sorted columns when possible
3. **Consider file preprocessing**: Remove unnecessary columns before comparison
4. **Use SSD storage**: Temporary file operations benefit from fast storage

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## Dependencies

- [Polars](https://pola.rs/) - Fast DataFrames for Rust
- [Clap](https://clap.rs/) - Command Line Argument Parser
- [Colored](https://crates.io/crates/colored) - Terminal color support
- [Indicatif](https://crates.io/crates/indicatif) - Progress bars

## Changelog

### Version 1.0.0
- Initial release
- Basic CSV comparison functionality
- Multi-column sorting support
- Detailed reporting with text and CSV output
- Performance optimizations for large files
- Fixed CSV escaping issues
