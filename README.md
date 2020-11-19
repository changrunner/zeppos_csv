# zeppos_csv

## Purpose
Working with csv should not be hard. This library simplifies it.

## install

### pip
```
pip install zeppos_csv
```

### pipenv
```
pipenv install zeppos_Csv
```

## Usage

```
from zeppos_logging.app_logger import AppLogger, AppLoggerJsonConfigName
from zeppos_bcpy.sql_configuration import SqlConfiguration
from zeppos_csv.csv_file import CsvFile

def main():
    csv_file = CsvFile(r"somefile.csv")
    sql_configuration = SqlConfiguration(
                server_type="microsoft",
                server_name="localhost\\sqlexpress",
                database_name="master",
                schema_name="dbo",
                table_name="staging_somefile"
            )
    csv_file.to_sql_server(
        pandas_dataframe=csv_file.get_dataframe_utf8_encoding_with_header(),
        sql_configuration=sql_configuration
    )


if __name__ == '__main__':
    AppLogger.configure_and_get_logger(
        logger_name='zeppos_test',
        config_section_name=AppLoggerJsonConfigName.default_with_watchtower_format_1(),
        watchtower_log_group="zeppos_test",
        watchtower_stream_name="local"
    )
    AppLogger.set_debug_level()
    main()

```