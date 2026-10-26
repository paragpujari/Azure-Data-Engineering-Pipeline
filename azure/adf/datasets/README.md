# ADF dataset templates
Create two datasets in ADF Studio and bind them to the pipeline's dataset names:

- `DS_RetailSales_Source`: DelimitedText dataset on your chosen source linked service; parameter `fileName`.
- `DS_RetailSales_Bronze`: DelimitedText dataset on ADLS Gen2 linked service; parameters `folderPath` and `fileName`.

The pipeline JSON is a starter export template, not a drop-in deployment. Validate dataset parameter names and source/sink settings in ADF Studio after importing/adapting it.
