# Lazy-codegen

Heavily 'inspired' by dbt-generator. Ok, I cloned the entire thing. I only take credit for the macros. The rest is all Tuan Chris. 

The only reason I'm not PR this to dbt-generator  is because it's a public repo, and my code is far too spaghetti to have our CTO review it. I like Joon, and rather not risk my job security like that. 


For sources with 10+ models, this package will save you a lot of time by generating base models in bulk and transform them for common fields automatically - casting timestamp field to assigned timezone, change boolean column_name  , and group columns together by data type. 

There is also the describe function allowing you to perform basic exploratory on your source tables. However, every describe function will run over your entire table, so ensure to use them with a condition clause. 
 
This currently works for Snowflake and Bigquery, for dbt version 1.0.8 and up. 


## Installation

To use this package, you need dbt installed with a profile configured. You will also need to copy the lazy_codgen folder from this repo to your macro folder.  


Install the package in the same environment with your dbt installation by running: 

```bash
pip install -e /your/directory
```

This package should be executed inside your dbt repo. 

Note: If you want to use the lint function, you'll need Sqlfluff installed. 

## Generate base models

To generate base models, use the `dbt-generator generate` command. This is a wrapper around the `base_table_gen` macro that will generate the base models. It will then process to generate every table at once. You can limit/specify the ammount of model generated through either the -m tags or through the --source-index option ( input the starting line of the model you want to generate)

```
Usage: dbt-generator generate [OPTIONS]

  Gennerate base models based on a .yml source

Options:      
  -s, --source-yml PATH             Source .yml file to be used
  -o, --output-path PATH            Path to write generated models
  -t, --timezone STRING             Timezone to convert detected timestamp columns too. Cannot convert columns that are not detected as timestamp. 
  -m, --model STRING                Model name. Genereate only specified models in inputted list. 
  -c, --custom_prefix STRING        Enter a Custom String Prefix for Model Filename
  --model-prefix BOOLEAN            optional prefix of source_name + "_" to the resulting modelname.sql to avoid model name collisions across sources 
  --source-index INTEGER            Index of the source to generate base models for. 
  -l, --linting  FLAG               Lint the generated table using sqlfluff. 
  -d, --describe FLAG               If enabled, will run the describe macro. Describe macro will generate a description table, as well as warn of    possible issues in the dataset (null/na check, distinctive columns, duplicate data within columns, etc. )
  -cr, --corr    FLAG               If enabled, will run the correlation macro. Generate a table of correlation between all number columns. 
  -dc, --describe-condition STRING  Query condition for desribe and correlation tables, to prevent excessive cost. Might create data skews. 
  --help                            Show    this message and exit.

```

### Example

```bash
dbt-generator generate -s ./models/source.yml -o ./models/staging/source_name/ - l -d
```

This will read in the `source.yml` file and generate the base models in the `staging/source_name` folder. If you have multiple sources defined in your `yml` file, use the `--source-index` flag to specify which source you want to start generating base models for.

## Base transformation

Tranformation are handled automatically using the base_table_gen macro. 

Custom transformation can be added by: 
- Adding a new custom list + column detection condition
- Adding new parsing command within the generation file 
- Detailed customization guide will be updated 
