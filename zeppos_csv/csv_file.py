import pandas as pd
import sys
from zeppos_core.timer import Timer
from zeppos_file_manager.file import File
from zeppos_logging.setup_logger import logger

class CsvFile(File):
    def __init__(self, full_file_name, extension="csv"):
        super().__init__(full_file_name)
        self._timer = Timer()

    def get_dataframe_windows_encoding_with_chunking(self, batch_size=1000):
        self._timer.start_timer()

        try:
            # https://pythonspeed.com/articles/faster-pandas-dask/
            df = pd.read_csv(self._full_file_name, iterator=True, chunksize=batch_size, dtype=str,
                             encoding="ISO-8859-1",
                             engine='python')

            self._timer.stop_timer()
            logger.debug(f"==> Read the csv Time: {self._timer.time_elapsed_in_seconds}")

            return df
        except Exception as e:
            logger.error(f"Error in core.csv_file.GetDataFrameWindowsEncoding: [{e}] - LineNo: [{sys.exc_info()[-1].tb_lineno}]")
            return None

    def get_dataframe_utf8_encoding_with_header(self, column_data_type_dict=None, low_memory=True):
        self._timer.start_timer()
        try:
            logger.debug(f"==> Read file [{self.full_file_name}] into dataframe")
            df = pd.read_csv(self._full_file_name, dtype=column_data_type_dict, low_memory=low_memory)

            self._timer.stop_timer()
            logger.debug(f"==> Read the csv Time: {self._timer.time_elapsed_in_seconds}")

            return df
        except Exception as e:
            logger.error(f"Error in core.csv_file.GetDataFrameUTF8Encoding: [{e}] - LineNo: [{sys.exc_info()[-1].tb_lineno}]")
            return None

    def get_dataframe_utf8_encoding_without_header(self, column_name_list, low_memory=True):
        self._timer.start_timer()
        try:
            df = pd.read_csv(self._full_file_name, header=None,
                             low_memory=low_memory, names=column_name_list)

            self._timer.stop_timer()
            logger.debug(f"==> Read the csv Time: {self._timer.time_elapsed_in_seconds}")

            return df
        except Exception as e:
            logger.error(f"Error in core.csv_file.GetDataFrameUTF8Encoding: [{e}] - LineNo: [{sys.exc_info()[-1].tb_lineno}]")
            return None

    def to_sql_server(self, table_schema, table_name, use_existing_sql_table, sql_server):
        pass


