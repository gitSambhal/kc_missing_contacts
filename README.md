# Contact Comparison Tool

A powerful Node.js tool to compare contact lists across multiple file formats and identify missing contacts.

## Features

- Supports multiple file formats:
  - VCF (vCard)
  - CSV
  - Excel (XLSX, XLS)
- Smart phone number standardization
- Batch processing for large files
- Detailed progress tracking
- Generates missing contacts in VCF format
- Exports detailed JSON logs
- Filters spam contacts automatically

## Setup

1. Create two directories in the project root:

   - `master_files/` - Place your main contact files here
   - `compare_files/` - Place the contact files to compare against

2. Install dependencies:

   ```bash
   npm install
   ```

3. Run the comparison:

   ```bash
   npm start
   ```

## Monitor Progress

- Watch real-time processing updates in the console.
- View file-by-file progress.
- See final statistics in a table format.

## Output Files

- **missing-contacts.vcf**: Contains all missing contacts in VCard format.
- **master_numbers.json**: Log of all processed master contacts.
- **compare_numbers.json**: Log of all processed comparison contacts.
- **missing_numbers.json**: Detailed log of missing contacts.

## Supported Column Names

The tool automatically recognizes various common phone and name column formats.

### Phone Columns

- kc_phone
- phone
- mobile
- cell
- telephone
- tel
- contact
- phone_value
- phone_1_value through phone_16_value
- car_phone
- primary_phone
- business_phone
- business_phone_2
- home_phone
- home_phone_2
- other_phone
- company_main_phone

### Name Columns

- kc_name
- name
- first_name
- last_name
- given_name
- short_name
- maiden_name
- middle_name
- family_name
- additional_name
- yomi_name
- given_name_yomi
- additional_name_yomi
- family_name_yomi
- nickname
- real_name

## Performance

- Processes files in batches of 1000 records.
- Uses streaming for memory-efficient processing.
- Handles large contact databases efficiently.

## Contact Filtering

The tool automatically filters out contacts with names containing:

- **spam**

## Phone Number Standardization

The tool performs the following standardization on phone numbers:

- Removes all non-digit characters except `+`.
- Handles multiple phone number formats.
- Removes common prefixes (91, 0, 00, +91, 091).
- Validates number length (minimum 10 digits).
- Extracts the first valid number from concatenated numbers.

## Error Handling

- Detailed error logging for each file.
- Skips records with parsing errors.
- Continues processing even if individual files fail.
