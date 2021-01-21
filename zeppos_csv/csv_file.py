import pandas as pd
import sys
from zeppos_core.timer import Timer
from zeppos_file_manager.file import File
from zeppos_logging.app_logger import AppLogger
from zeppos_bcpy.dataframe import Dataframe
import os

class CsvFile(File):
    def __init__(self, full_file_name):
        super().__init__(full_file_name)
        self._timer = Timer()

    @staticmethod
    def create_csv_file_instance_with_todays_date(root_directory, file_name, format="%Y_%m_%d_%H_%M_%S"):
        return File.create_instance_with_todays_date(root_directory, file_name, CsvFile, format)

    def get_dataframe_windows_encoding_with_header(self, low_memory=True, sep="|"):
        return self._get_dataframe(encoding='windows', has_header=True, read_in_chunks=False, low_memory=low_memory, sep=sep)

    def get_dataframe_windows_encoding_with_header_and_chunking(self, batch_size=1000, sep="|"):
        return self._get_dataframe(encoding='windows', has_header=True, read_in_chunks=True, batch_size=batch_size, sep=sep)

    def get_dataframe_windows_encoding_without_header(self, column_name_list, low_memory=True, sep="|"):
        return self._get_dataframe(encoding='windows', has_header=False, read_in_chunks=False,
                                   column_name_list=column_name_list, low_memory=low_memory, sep=sep)

    def get_dataframe_windows_encoding_without_header_and_chunking(self, column_name_list, batch_size=1000, sep="|"):
        return self._get_dataframe(encoding='windows', has_header=False, read_in_chunks=True,
                                   column_name_list=column_name_list, batch_size=batch_size, sep=sep)

    def get_dataframe_utf8_encoding_with_header(self, low_memory=True, sep="|"):
        return self._get_dataframe(encoding='utf-8', has_header=True, read_in_chunks=False, low_memory=low_memory, sep=sep)

    def get_dataframe_utf8_encoding_with_header_and_chunking(self, low_memory=True, sep="|"):
        return self._get_dataframe(encoding='utf-8', has_header=True, read_in_chunks=True, low_memory=low_memory, sep=sep)

    def get_dataframe_utf8_encoding_without_header(self, column_name_list, low_memory=True, sep="|"):
        return self._get_dataframe(encoding='utf-8', has_header=False, read_in_chunks=False,
                                   column_name_list=column_name_list, low_memory=low_memory, sep=sep)

    def get_dataframe_utf8_encoding_without_header_and_chunking(self, column_name_list, low_memory=True, sep="|"):
        return self._get_dataframe(encoding='utf-8', has_header=False, read_in_chunks=True,
                                   column_name_list=column_name_list, low_memory=low_memory, sep=sep)

    def _get_dataframe(self, encoding, has_header, read_in_chunks, column_name_list=None, batch_size=10000, low_memory=True, sep="|"):
        self._timer.start_timer()
        df = None
        try:
            if encoding == 'utf-8':
                if has_header:
                    if read_in_chunks:
                        df = pd.read_csv(self._full_file_name, iterator=True, chunksize=batch_size, dtype=object,
                                         low_memory=low_memory, sep=sep)
                    else:
                        df = pd.read_csv(self._full_file_name, dtype=object, low_memory=low_memory, sep=sep)
                else:
                    if read_in_chunks:
                        df = pd.read_csv(self._full_file_name, iterator=True, chunksize=batch_size, header=None,
                                         names=column_name_list, dtype=object, low_memory=low_memory, sep=sep)
                    else:
                        df = pd.read_csv(self._full_file_name, header=None, names=column_name_list,
                                         dtype=object, low_memory=low_memory, sep=sep)
            elif encoding == 'windows':
                if has_header:
                    if read_in_chunks:
                        df = pd.read_csv(self._full_file_name, iterator=True, chunksize=batch_size, dtype=object,
                                         encoding="ISO-8859-1", engine='python', sep=sep)
                    else:
                        df = pd.read_csv(self._full_file_name, dtype=object, encoding="ISO-8859-1", engine='python',
                                         low_memory=low_memory, sep=sep)
                else:
                    if read_in_chunks:
                        df = pd.read_csv(self._full_file_name, iterator=True, chunksize=batch_size, header=None,
                                         names=column_name_list, dtype=object, encoding="ISO-8859-1", engine='python',
                                         sep=sep)
                    else:
                        df = pd.read_csv(self._full_file_name, header=None, names=column_name_list, dtype=object,
                                         encoding="ISO-8859-1", engine='python', low_memory=low_memory, sep=sep)
            self._timer.stop_timer()
            AppLogger.logger.debug(f"==> Read the csv Time: {self._timer.time_elapsed_in_seconds}")

        except Exception as e:
            AppLogger.logger.error(
                f"Error in core.csv_file.GetDataFrameUTF8Encoding: [{e}] - LineNo: [{sys.exc_info()[-1].tb_lineno}]")

        return df

    @staticmethod
    def to_sql_server(pandas_dataframe, sql_configuration, additional_static_data_dict=None,
                      use_existing=False, csv_full_file_name=None):
        dataframe = Dataframe.to_sqlserver_creating_instance(pandas_dataframe=pandas_dataframe,
                                                             sql_configuration=sql_configuration,
                                                             additional_static_data_dict=additional_static_data_dict,
                                                             use_existing=use_existing,
                                                             csv_full_file_name=csv_full_file_name)
        return {"columns": dataframe.pandas_dataframe.columns.to_list()}

    def to_sql_server_with_chunking(self, pandas_dataframe_chunks, sql_configuration, use_existing=False):
        self._timer.start_timer()

        total_record_count = 0
        chunk_counter = 1
        error = None
        return_dict = None
        for pandas_dataframe_chunk in pandas_dataframe_chunks:
            AppLogger.logger.debug(f"Chunk No: {chunk_counter}")
            AppLogger.logger.debug(f"CSV File Name: {self.file_name}")
            AppLogger.logger.debug(f"Error Occured?: {error is not None}")
            if not error:
                try:
                    total_record_count += pandas_dataframe_chunk.shape[0]

                    return_dict = self.to_sql_server(
                        pandas_dataframe=pandas_dataframe_chunk,
                        sql_configuration=sql_configuration,
                        use_existing=use_existing,
                        csv_full_file_name=self.full_file_name
                    )

                    use_existing = True
                except Exception as e:
                    AppLogger.logger.error(f"===> Error Occured. See further detail later in this log. {e}")
                    error = e
                    total_record_count = 0

            chunk_counter += 1

        self._timer.stop_timer()
        if error:
            AppLogger.logger.info(f"Error => No records processed in seconds: {self._timer.time_elapsed_in_seconds}")
        else:
            AppLogger.logger.info(
                f"Processing of [{total_record_count}] records to load Csv into Sql Server in seconds: {self._timer.time_elapsed_in_seconds}")

        return return_dict

    def save_dataframe(self, df, sep="|"):
        try:
            AppLogger.logger.debug("Saving dataframe to file.")
            os.makedirs(os.path.dirname(self.full_file_name), exist_ok=True)
            df.to_csv(self.full_file_name, sep=sep, index=False)
            AppLogger.logger.debug("Saved dataframe to file.")
            return True
        except Exception as error:
            AppLogger.logger.error(f"Error occured during csv_file.save_dataframe: {error}")
            return False

