from zeppos_logging.app_logger import AppLogger
from zeppos_file_manager.files import Files
from zeppos_csv.csv_file import CsvFile
from os import path, makedirs
import pandas as pd


class CsvFiles(Files):
    def __init__(self, base_dir, extension="csv", start_file_filter=None, end_file_filter=None,
                 include_processed=False, include_subdir=False):
        super().__init__(
            base_dir=base_dir,
            extension=extension,
            start_file_filter=start_file_filter,
            end_file_filter=end_file_filter,
            include_processed=include_processed,
            file_object=CsvFile,
            include_subdir=include_subdir
        )

    def to_sql_server(self, sql_configuration, use_existing=False, low_memory=True,
                      sep="|", mark_as_processed=False):
        return_dict = {'error': None}
        for csv_file in self.__iter__():
            try:
                AppLogger.logger.debug(f"csv_file type: {type(csv_file)}")
                csv_file.to_sql_server(
                    pandas_dataframe=csv_file.get_dataframe_utf8_encoding_with_header(low_memory=low_memory, sep=sep),
                    sql_configuration=sql_configuration.validate_and_augment(
                        file_name_without_extension=csv_file.file_name_without_extension
                    ),
                    use_existing=use_existing,
                    additional_static_data_dict=csv_file.additional_data_from_directory
                )
                use_existing = True  # set to True so we don't keep creating the table.

                if mark_as_processed:
                    csv_file.mark_as_done()
            except Exception as error:
                return_dict['error'] = error
                AppLogger.logger.error(f"Error to_sql_server: {error}")
                if mark_as_processed:
                    csv_file.mark_as_fail()
        return return_dict

    def to_sql_server_with_chunking(self, sql_configuration, use_existing=False, low_memory=True,
                                    sep="|", mark_as_processed=False):
        return_dict = {'error': None}
        for csv_file in self.__iter__():
            try:
                AppLogger.logger.debug(f"csv_file type: {type(csv_file)}")
                csv_file.to_sql_server_with_chunking(
                    pandas_dataframe_chunks=csv_file.get_dataframe_utf8_encoding_with_header_and_chunking(
                        low_memory=low_memory,
                        sep=sep),
                    sql_configuration=sql_configuration,
                    use_existing=use_existing
                )
                use_existing = True  # set to True so we don't keep creating the table.

                if mark_as_processed:
                    csv_file.mark_as_done()
            except Exception as error:
                return_dict['error'] = error
                AppLogger.logger.error(f"Error to_sql_server_with_chunking: {error}")
                if mark_as_processed:
                    csv_file.mark_as_fail()
        return return_dict

    def get_dataframe_utf8_encoding_with_header(self, column_data_type_dict=None, low_memory=True):
        df_final = pd.DataFrame()
        for csv_file in self.__iter__():
            df_final = pd.concat([df_final,
                                  csv_file.get_dataframe_utf8_encoding_with_header(
                                      low_memory=low_memory
                                  )
                                  ])
        return df_final

    def get_dataframe_utf8_encoding_without_header(self, column_name_list, low_memory=True):
        df_final = pd.DataFrame()
        for csv_file in self.__iter__():
            df_final = pd.concat([df_final,
                                  csv_file.get_dataframe_utf8_encoding_without_header(
                                      column_name_list=column_name_list,
                                      low_memory=low_memory
                                  )
                                  ])
        return df_final

    def combine_csv_files_utf8_encoding_with_header(self, target_csv_full_file_name, mark_as_done=True):
        if not path.exists(path.dirname(target_csv_full_file_name)):
            makedirs(path.dirname(target_csv_full_file_name))

        for csv_file in self.__iter__():
            AppLogger.logger.debug(f'Add file to dataframe: {csv_file.file_name}')
            df = csv_file.get_dataframe_utf8_encoding_with_header()
            if path.isfile(target_csv_full_file_name):
                df.to_csv(target_csv_full_file_name, mode='a', header=False, index=False)
            else:
                df.to_csv(target_csv_full_file_name, header='column_names', index=False)

            if mark_as_done:
                csv_file.mark_as_done()

        return True
