When you run a `SELECT count(*)` query on a **materialized (native)** BigQuery table, the estimated bytes to be read is **0 B**.

---

### ## Why it estimates 0 B

BigQuery is able to return the total row count of a native table for free because it doesn't actually scan your data to get the answer.

Instead, it pulls this information from the **Table Metadata**.

* **Metadata Storage:** BigQuery maintains a set of system-level statistics for every native table, which includes an exact, real-time `row_count`.
* **Optimization:** When you run a naked `COUNT(*)`, the query optimizer realizes it can fulfill the request just by looking at the table's "header" or metadata record rather than opening the columnar data files.
* **Billing Rule:** Since no data columns are physically scanned from the Colossus storage system, BigQuery reports **0 bytes processed**.

### ## Contrast with other table types

It is important to note that this **0 B** "magic" only works for specific scenarios:

* **External Tables:** Often show **0 B** in the estimate because BigQuery doesn't own the metadata for files in GCS, but it might still incur costs or processing time once run to count the rows in the files.
* **Filtered Counts:** If you add a filter (e.g., `WHERE VendorID = 1`), BigQuery can no longer use the table-level metadata. It must scan the `VendorID` column to evaluate the condition, and you will see a non-zero byte estimate.

**Would you like to move on to the final part of the homework, which usually covers how to manage your tables and clean up your GCP environment?**