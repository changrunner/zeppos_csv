import unittest
from zeppos_csv.csv_files import CsvFiles
from tests.util_for_testing import UtilForTesting
from zeppos_bcpy.sql_configuration import SqlConfiguration
from zeppos_logging.app_logger import AppLogger
import os


class TestProjectMethods(unittest.TestCase):
    def setUp(self):
        UtilForTesting.file_clean_up()

    def tearDown(self):
        UtilForTesting.file_clean_up()

    def test_constructor_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_1')
        self.assertEqual(str(type(CsvFiles(file_dir))), "<class 'zeppos_csv.csv_files.CsvFiles'>")

    def test_to_sql_server_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_8', extension="",
                                                                            content="col1,col2\ntest1,test2")
        return_dict = CsvFiles(file_dir).to_sql_server(
            sql_configuration=SqlConfiguration(
                server_type="microsoft",
                server_name="localhost\\sqlexpress",
                database_name="master",
                schema_name="dbo",
                table_name="staging_test_to_sql_server_3"
            ),
            sep=","
        )
        # if the above statement ran ok then we assume all is ok.
        self.assertEqual(None, return_dict['error'])

    def test_to_sql_server_with_additional_information_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup(r'test_df_9\field1=somedata1\field2=somedata2', extension="",
                                                                            content="col1,col2\ntest1,test2")
        return_dict = CsvFiles(file_dir).to_sql_server(
            sql_configuration=SqlConfiguration(
                server_type="microsoft",
                server_name="localhost\\sqlexpress",
                database_name="master",
                schema_name="dbo",
                table_name="staging_test_to_sql_server_4"
            )
        )
        # if the above statement ran ok then we assume all is ok.
        self.assertEqual(None, return_dict['error'])

    def test_to_sql_server_no_table_name_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup(r'test_df_10', extension="",
                                                                            content="col1,col2\ntest1,test2")
        return_dict = CsvFiles(file_dir).to_sql_server(
            sql_configuration=SqlConfiguration(
                server_type="microsoft",
                server_name="localhost\\sqlexpress",
                database_name="master",
                schema_name="dbo",
                table_name=None
            )
        )
        # if the above statement ran ok then we assume all is ok.
        self.assertEqual(None, return_dict['error'])

    def test_to_sql_server_with_chunking_method(self):
        AppLogger.configure_and_get_logger("test_logger")
        AppLogger.set_debug_level()
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_8', extension="",
                                                                            content="col1|col2\ntest1|test2")
        return_dict = CsvFiles(file_dir).to_sql_server_with_chunking(
            sql_configuration=SqlConfiguration(
                server_type="microsoft",
                server_name="localhost\\sqlexpress",
                database_name="master",
                schema_name="dbo",
                table_name="staging_test_to_sql_server_chunking"
            )
        )
        # if the above statement ran ok then we assume all is ok.
        self.assertEqual(None, return_dict['error'])

    def test_1_get_dataframe_utf8_encoding_with_header_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_2', extension="",
                                                                            content="col1,col2\ntest1,test2")
        df = CsvFiles(file_dir, 'csv').get_dataframe_utf8_encoding_with_header()
        self.assertEqual(df.shape[0], 1)

    def test_2_get_dataframe_utf8_encoding_with_header_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_3', extension="",
                                                                            content="col1,col2\ntest1,test2",
                                                                            count=2)
        df = CsvFiles(file_dir, 'csv').get_dataframe_utf8_encoding_with_header()
        self.assertEqual(df.shape[0], 2)

    def test_1_get_dataframe_utf8_encoding_without_header_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_4',
                                                                            extension="",
                                                                            content="test1,test2",
                                                                            count=1)
        df = CsvFiles(file_dir, 'csv').get_dataframe_utf8_encoding_without_header(['col1', 'col2'])
        self.assertEqual(df.shape[0], 1)

    def test_2_get_dataframe_utf8_encoding_without_header_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_5',
                                                                            extension="",
                                                                            content="test1,test2",
                                                                            count=2)
        df = CsvFiles(file_dir, 'csv').get_dataframe_utf8_encoding_without_header(['col1', 'col2'])
        self.assertEqual(df.shape[0], 2)

    def test_1_combine_csv_files_utf8_encoding_with_header_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_6',
                                                                            extension="",
                                                                            content="col1,col2\ntest1,test2",
                                                                            count=2)
        target_file_name = os.path.join(temp_dir, 'test_df_2.csv')
        self.assertEqual(CsvFiles(file_dir).combine_csv_files_utf8_encoding_with_header(target_file_name), True)
        self.assertEqual(os.path.exists(target_file_name), True)
        self.assertEqual(os.path.exists(full_file_name_list[0] + ".done"), True)
        self.assertEqual(os.path.exists(full_file_name_list[1] + ".done"), True)
        with open(target_file_name) as fl:
            content = fl.read()
        self.assertEqual(len(content.split("\n")), 4)

    def test_2_combine_csv_files_utf8_encoding_with_header_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_7',
                                                                            extension="",
                                                                            content="col1,col2\ntest1,test2",
                                                                            count=2)
        target_file_name = os.path.join(temp_dir, 'test_df_2.csv')
        self.assertEqual(CsvFiles(file_dir).combine_csv_files_utf8_encoding_with_header(target_file_name, mark_as_done=False), True)
        self.assertEqual(os.path.exists(target_file_name), True)
        self.assertEqual(os.path.exists(full_file_name_list[0]), True)
        self.assertEqual(os.path.exists(full_file_name_list[1]), True)
        with open(target_file_name) as fl:
            content = fl.read()
        self.assertEqual(len(content.split("\n")), 4)


if __name__ == '__main__':
    unittest.main()
