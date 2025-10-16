# Project Plan

## Information

- **Configuration:**  
  Many files in this repository rely on `variables.py` to configure your environment and determine where synthetic data is generated. To change the catalog name or other object names, simply update them in `variables.py`.
- **Resource Limits:**  
  If you see a "Resources exhausted" error:
  - Stop the `data_generator.py` script if it's running.
  - Wait a short time before trying again.
  - _Note: This is a limitation of Databricks Free Edition._
- **Help & Inspiration:**  
  If you encounter challenges or uncertainties:
  - A complete reference implementation is available in the "final dlt pipeline" folder.
  - Reviewing the final code can clarify requirements, demonstrate best practices, and guide your pipeline structure.
  - Use this resource to:
    - Compare your approach
    - Troubleshoot issues
    - Deepen your understanding of Delta Live Tables and Databricks workflows

---

## Databricks Configuration Requirements

> **Tip:** If you are new to Databricks, take time to explore the workspace UI and documentation. Understanding catalogs, schemas, and volumes will help you later.

To run this project end-to-end, complete the following setup steps in your Databricks workspace:

1. **Create a Databricks Account**

   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don’t already have one.
   - Familiarize yourself with the workspace, clusters, and notebook interface.

2. **Import this repository to Databricks**

   - In Databricks, go to the Workspace sidebar and click the "Repos" section, click "Add Repo".
     - Alternatively, go to your personal folder, click "create" and select "git folder".
   - Paste the GitHub URL for this repository.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: [Repos in Databricks](https://docs.databricks.com/repos/index.html).

3. **Run the "setup_environment" notebook to set up a catalog, schemas and volumes for the synthetic data generator**. It will use the paths defined in the variables file for creating appropriate objects in Unity Catalog.

4. **Create and Configure a DLT Pipeline**

   - In Databricks, create a new Delta Live Tables (DLT) pipeline.
   - Set the pipeline to use your project folder (containing the DLT code) as the source.
   - Set the default catalog to `apparel_store`.
   - Set the target schema to, for example, `02_silver` (or as appropriate for your workflow).
   - Configure cluster and permissions as needed.
   - Set the source folder to this folder, and the source code as the "your_dlt_pipeline" file.
   - **Tip:** Review the DLT pipeline settings and documentation. Understand the difference between streaming and batch tables.

5. **Run the synthetic data generator (`data_generator.py`) to initialize some data.**
   - It will stream infinitely until stopped, so stop it after a few minutes.
   - **Tip:** Check the output location and schema of the generated data. Use the provided markdown for table schemas and data quirks.

---

## DLT Pipeline Reconstruction Checklist

> **How to approach the checklist:**  
> For each task, think about the business logic, data quality, and transformation required. Use the synthetic data generator's quirks (see [SynteticDataGenerator.md](SynteticDataGenerator.md)) to inform your design. It also covers schema of the synthetic data. Try to reason about why each expectation or transformation is needed.
