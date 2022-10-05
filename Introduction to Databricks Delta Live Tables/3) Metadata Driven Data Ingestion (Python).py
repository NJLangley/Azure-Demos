# Databricks notebook source
# MAGIC %pip install PyYAML

# COMMAND ----------

# MAGIC %md
# MAGIC > ***Note:*** *Update the paths for the data lake connection before running on your own environment!*

# COMMAND ----------

# MAGIC %md
# MAGIC # 3) Metadata Driven Data ingestion Using Python
# MAGIC Drive table creating using metadata in a YAML file in the git repo
# MAGIC > ***Note:*** *Any pip install commands to install required modules must be the first cell in the notebook*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debugging helper
# MAGIC Debugging DLT is a bit painful, we can't import the `dlt` module, so we bounce back and forth between writing code and running the pipeline, or we comment out the DLT bits and add comments so we can run the notebook. This gets worse when developing metadata driven pipelines.
# MAGIC A better way is to use dbutils to get the current context as JSON, and then look for the `browserLanguage` tag. This is only there when the notebook is open for editing/running.
# MAGIC 
# MAGIC This function returns a bool indicating if we are in edit mode. We can put all the DLT bits behind an if statement that calls this, and now we can both run the notebook to check paths etc, and run it as a DLT pipline with no changes!

# COMMAND ----------

import json
def is_debug_mode():
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    tags = json.loads(context).get('tags')
    if tags:
        lang = tags.get('browserLanguage')
        if lang:
            return True
    return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import python modules
# MAGIC These are split so we don't import `dlt` when testing in the notebook mode
# MAGIC > **Note:** Autocomplete for python does not work until you run a cell against a normal cluster. However DLT notebooks cannot be run on a normal cluster, so run the single cell below to get autocomplete (somewhat) working 

# COMMAND ----------

if not is_debug_mode():
    import dlt

# COMMAND ----------

from pyspark.sql.functions import *
print('Autocomplete should work now')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define classes & functions to build DLT tables and views
# MAGIC We have a YAML file stored in the Git repo along side this notebook. It contains simple mappings for building the bronze layer, and a flag to determin if we should add a silver table. It also has dictionaries for the expectations, and the required CDC handling (SCD type)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Classes
# MAGIC Simple class representations of the metadata.

# COMMAND ----------

from dataclasses import dataclass
import dataclasses
from typing import Callable
import json

@dataclass
class DltEntity:
    """A representation of a Delta Live Tables entity."""
    dlt_type: str
    entity_name: str
    layer: str
    root_path: str
    
    @property
    def table_name(self):
        return f'{self.layer}_{self.entity_name}'
    
    @property
    def comment(self):
        return f"The Adventure Works {self.layer} {self.dlt_type} '{self.table_name}'"
    
    @property
    def path(self):
        return f'{self.root_path}/{self.table_name}'

    @property
    def decorator(self) -> Callable:
        if not is_debug_mode():
            if self.dlt_type == 'view':
                return dlt.view
            else:
                return dlt.table
        if is_debug_mode():
            if self.dlt_type == 'view':
                return '@dlt.view'
            else:
                return '@dlt.table'
            
    @property
    def decorator_args(self) -> dict:
        decorator_args = {
            'name': self.table_name,
            'comment': self.comment
        }
        if self.dlt_type == 'view':
            return decorator_args
        else:
            decorator_args['path'] = self.path
            return decorator_args
    
    def print_values(self):
        # Only print in debug mode, as functions returned by the decorator property are not serializable
#         if is_debug_mode():
        properties = [k for t in type(self).mro() for k,v in vars(t).items() if t != object and type(v) is property]
        values_dict = dataclasses.asdict(self)
        for prop in properties:
            val = getattr(self, prop)
            # Skip function variable as they are not serializable!
            if not callable(val):
                if dataclasses.is_dataclass(val):
                    val = dataclasses.asdict(val)
                values_dict[prop] = val
        print(json.dumps(values_dict, indent = 4))

@dataclass
class SourceDltEntity(DltEntity):
    """A representation of a Delta Live Tables entity loaded with autoloader."""
    file_format: str
    options: dict
    landed_root_path: str
    
    @property
    def landed_path(self):
        return f'{self.landed_root_path}/{self.file_format}/{self.entity_name}'
    
@dataclass 
class CdcConfig:
    stored_as_scd_type: str
    keys: list
    sequence_by: str
    column_list: list
    except_column_list: list
    
@dataclass
class TargetDltEntity(DltEntity):
    """A representation of a Delta Live Tables entity loaded from another live table."""
    expect: list
    expect_or_drop: list
    expect_or_fail: list
    cdc_config: CdcConfig
    
    @property
    def source_table_name(self):
        return f'Bronze_{self.entity_name}'
  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Classes Builders
# MAGIC The instantiate the Dataclass instances above from the YAML metadata. These could be reimplemented to get metadata from another source, JSON, DB, etc...

# COMMAND ----------

def build_cdc_config(config):
    if not config:
        return None
    
    stored_as_scd_type = config.get('scd_type')
    if not stored_as_scd_type:
        return None
    
    keys = config.get('keys')
    sequence_by = config.get('sequence_by')
    column_list = config.get('column_list')
    except_column_list = config.get('except_column_list')
    
    return CdcConfig(stored_as_scd_type, keys, sequence_by, column_list, except_column_list)

def build_dlt_entity(table, layer, config, target_root_path, landed_root_path=None):
    dlt_type = config.get('dlt_type')
    file_format = config.get('format')
    options = config.get('options', {})
    expect = config.get('expect', {})
    expect_or_drop = config.get('expect_or_drop', {})
    expect_or_fail = config.get('expect_or_fail', {})
    cdc_config = config.get('cdc_config', {})
    
    if file_format:
        return SourceDltEntity(dlt_type, table, layer, target_root_path, file_format, options, landed_root_path)
    else:
        cdc_config = build_cdc_config(config.get('cdc'))
        return TargetDltEntity(dlt_type, table, layer, target_root_path, expect, expect_or_drop, expect_or_fail, cdc_config)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Genric DLT Table/View Definitions
# MAGIC These use the data classes above to build DLT tables from the metadata in a genric manner

# COMMAND ----------

def create_bronze(table, dlt_config: SourceDltEntity):
    if not is_debug_mode():
        decorator = dlt_config.decorator
        decorator_args = dlt_config.decorator_args

        @decorator(**decorator_args)
        def bronze():
            df = (spark.readStream.format("cloudFiles")
                     .option('cloudFiles.format', dlt_config.file_format)
                     .options(**dlt_config.options)
                     .load(dlt_config.landed_path)
                     .withColumn("source_filename", input_file_name())
               )
            return df

# COMMAND ----------

def create_silver(table, dlt_config: TargetDltEntity):
    if not is_debug_mode():
        decorator = dlt_config.decorator
        decorator_args = dlt_config.decorator_args

        if not dlt_config.cdc_config:
            @decorator(**decorator_args)
            @dlt.expect_all(dlt_config.expect)
            @dlt.expect_all_or_drop(dlt_config.expect_or_drop)
            @dlt.expect_all_or_fail(dlt_config.expect_or_fail)
            def sliver():
                return dlt.read_stream(dlt_config.source_table_name)

        else:
            dlt.create_streaming_live_table(**decorator_args)
            
            cdc_tables = {
                'target': dlt_config.table_name,
                'source': dlt_config.source_table_name
            }
            cdc_args = cdc_tables | dataclasses.asdict(dlt_config.cdc_config)
            dlt.apply_changes(**cdc_args)

# COMMAND ----------

import yaml
def get_yaml_config(config_path):
    # If the config file is on the lake copy it to a temp folder on dbfs so we can read it
    if config_path.startswith('abfss://'):
        filename = config_path.split('/')[-1]
        dbfs_path = f'dbfs:/tmp/{filename}'
        dbutils.fs.cp(config_path, dbfs_path)

        config_path = dbfs_path.replace('dbfs:/', '/dbfs/')

    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load the config file, and create the tables
# MAGIC Pretty much verything above here is generic and could be put in a library to keep the notebook clean

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set the storage account and container in one place
# MAGIC We attempt to get these from spark config, but have defaults to fall back on fo testing

# COMMAND ----------

storage_account = spark.conf.get('dlt_metadata.storage_account',         'puddle')
storage_container = spark.conf.get('dlt_metadata.storage_container',     'demo-lake')
landed_root_path = spark.conf.get('dlt_metadata.landed_root_path',       'adventure-works/landed')
lake_root_path = spark.conf.get('dlt_metadata.lake_root_path',           'adventure-works/Demo3')
yaml_metadata_path = spark.conf.get('dlt_metadata.yaml_metadata_path',   '/Workspace/Repos/niall.langley@redkite.com/Azure-Demos-Private/Introduction to Databricks Delta Live Tables/3-data-ingestion.yml')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Build the intermediate paths

# COMMAND ----------

storage_root = f'abfss://{storage_container}@{storage_account}.dfs.core.windows.net'
landed_root_path = f'{storage_root}/{landed_root_path}'
lake_root_path = f'{storage_root}/{lake_root_path}'

bronze_root_path = f'{lake_root_path}/bronze'
silver_root_path = f'{lake_root_path}/silver'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Build the full DLT graph. 
# MAGIC If we run the notebook directly, this will output JSON showing what will be submitted to the DLT process to build the tables
# MAGIC If the notebook is run from a DLT workflow, it will loop over the list of tables and build DLT tables for us

# COMMAND ----------

yaml_dict = get_yaml_config(yaml_metadata_path)

for table in yaml_dict['tables']:
    bronze_config = yaml_dict['tables'][table].get('bronze')
    if bronze_config:
        bronze_entity = build_dlt_entity(table, 'Bronze', bronze_config, bronze_root_path, landed_root_path)
        bronze_entity.print_values()
        create_bronze(table, bronze_entity)
    
    silver_config = yaml_dict['tables'][table].get('silver')
    if silver_config:
        silver_entity = build_dlt_entity(table, 'Silver', silver_config, silver_root_path)
        silver_entity.print_values()
        create_silver(table, silver_entity)
