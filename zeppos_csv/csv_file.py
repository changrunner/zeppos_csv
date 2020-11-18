import pandas as pd
import sys
from zeppos_core.timer import Timer
from zeppos_file_manager.file import File
from zeppos_logging.app_logger import AppLogger
from zeppos_bcpy.sql_configuration import SqlConfiguration
from zeppos_bcpy.dataframe import Dataframe

class CsvFile(File):
    def __init__(self, full_file_name):
        super().__init__(full_file_name)
        self._timer = Timer()

    def get_dataframe_windows_encoding_with_chunking(self, batch_size=1000):
        self._timer.start_timer()
        try:
            # https://pythonspeed.com/articles/faster-pandas-dask/
            df = pd.read_csv(self._full_file_name, iterator=True, chunksize=batch_size, dtype=object,
                             encoding="ISO-8859-1",
                             engine='python')

            self._timer.stop_timer()
            AppLogger.logger.debug(f"==> Read the csv Time: {self._timer.time_elapsed_in_seconds}")

            return df
        except Exception as e:
            AppLogger.logger.error(f"Error in core.csv_file.GetDataFrameWindowsEncoding: [{e}] - LineNo: [{sys.exc_info()[-1].tb_lineno}]")
            return None

    def get_dataframe_utf8_encoding_with_header(self, low_memory=True):
        self._timer.start_timer()
        try:
            AppLogger.logger.debug(f"==> Read file [{self.full_file_name}] into dataframe")
            df = pd.read_csv(self._full_file_name, dtype=object, low_memory=low_memory)

            self._timer.stop_timer()
            AppLogger.logger.debug(f"==> Read the csv Time: {self._timer.time_elapsed_in_seconds}")

            return df
        except Exception as e:
            AppLogger.logger.error(f"Error in core.csv_file.GetDataFrameUTF8Encoding: [{e}] - LineNo: [{sys.exc_info()[-1].tb_lineno}]")
            return None

    def get_dataframe_utf8_encoding_without_header(self, column_name_list, low_memory=True):
        self._timer.start_timer()
        try:
            df = pd.read_csv(self._full_file_name, header=None, names=column_name_list,
                             dtype=object, low_memory=low_memory)

            self._timer.stop_timer()
            AppLogger.logger.debug(f"==> Read the csv Time: {self._timer.time_elapsed_in_seconds}")

            return df
        except Exception as e:
            AppLogger.logger.error(f"Error in core.csv_file.GetDataFrameUTF8Encoding: [{e}] - LineNo: [{sys.exc_info()[-1].tb_lineno}]")
            return None

    @staticmethod
    def to_sql_server(pandas_dataframe, sql_configuration,
                      use_existing=False):
        dataframe = Dataframe.to_sqlserver_creating_instance(pandas_dataframe, sql_configuration,
                                                             use_existing=use_existing)
        return dataframe.pandas_dataframe

    # This will require some more work.
    # Todo: Complete this.
    # def to_sql_server_with_chunking(self, df_chunks, sql_configuration, use_existing=False):
    #     AppLogger.logger.debug("Entering SpinOff.Extractor.load_csv_info_sql_server")
    #
    #     self._timer.start_timer()
    #
    #     total_record_count = 0
    #     chunk_counter = 1
    #     error = None
    #     for df_chunk in df_chunks:
    #         AppLogger.logger.debug(f"Chunk No: {chunk_counter}")
    #         AppLogger.logger.debug(f"CSV File Name: {self.file_name}")
    #         AppLogger.logger.debug(f"Error Occured?: {error is not None}")
    #     #     if not error:
    #     #         try:
    #     #             df_staging = SpinOff.Transformer(self._logger).process(df_chunk, OPCO)
    #     #
    #     #             SpinOff.MetaData(self._logger).do_we_have_valid_columns(df_staging)
    #     #
    #     #             total_record_count += df_staging.shape[0]
    #     #
    #     #             self._sql_server.insert_using_bcp(
    #     #                 df=df_staging,
    #     #                 server_name=self._sql_server.server_name,
    #     #                 database_name=self._sql_server.database_name,
    #     #                 staging_table_schema=self._staging_table_schema,
    #     #                 staging_table_name=self._staging_table_name,
    #     #                 username=None,
    #     #                 password=None,
    #     #                 use_existing_sql_table=use_existing_sql_table)
    #     #
    #     #             use_existing_sql_table = True
    #     #         except Exception as e:
    #     #             self._logger.error(f"===> Error Occured. See further detail later in this log. {e}")
    #     #             error = e
    #     #             total_record_count = 0
    #     #
    #         chunk_counter += 1
    #
    #     self._timer.stop_timer()
    #     if error:
    #         self._logger.info(f"Error => No records processed in seconds: {self._timer.time_elapsed_in_seconds}")
    #     else:
    #         self._logger.info(
    #             f"Processing of [{total_record_count}] records to load Csv into Sql Server in seconds: {self._timer.time_elapsed_in_seconds}")


