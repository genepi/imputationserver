# Output Files

The Polygenic Score Calculation Results CSV file provides Polygenic Score (PGS) values for different samples and associated identifiers. 
Users can leverage this CSV file to analyze and compare Polygenic Score values across different samples. The data facilitates the investigation of genetic associations and their impact on specific traits or conditions.

## CSV Format

The CSV file consists of a header row and data rows:

### Header Row

- **sample**: Represents the identifier for each sample.
- **PGS000001, PGS000002, PGS000003, ...**: Columns representing different Polygenic Score values associated with the respective identifiers.

### Data Rows

- Each row corresponds to a sample and provides the following information:
    - **sample**: Identifier for the sample.
    - **PGS000001, PGS000002, PGS000003, ...**: Polygenic Score values associated with the respective identifiers for the given sample.

### Example

Here's an example row:

```csv
sample, PGS000001, PGS000002, PGS000003, ...
sample1, -4.485780284301654, 4.119604924228042, 0.0, -4.485780284301654
```

- **sample1**: Sample identifier.
    - **-4.485780284301654**: Polygenic Score value for `PGS000001`.
    - **4.119604924228042**: Polygenic Score value for `PGS000002`.
    - **0.0**: Polygenic Score value for `PGS000003`.

**Note:**

- Polygenic Score values are provided as floating-point numbers.
- The absence of values (e.g., `0.0`) indicates a lack of Polygenic Score information for a particular identifier in a given sample.
