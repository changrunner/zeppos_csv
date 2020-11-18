import unittest
from zeppos_csv.csv_file import CsvFile
from tests.util_for_testing import UtilForTesting
from zeppos_bcpy.sql_configuration import SqlConfiguration
import os


class TestProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        self.assertEqual(str(type(CsvFile(r"c:\temp\test1.csv"))), "<class 'zeppos_csv.csv_file.CsvFile'>")

    def test_get_dataframe_windows_encoding_with_chunking_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_1', content="col1,col2\ntest1,test2")
        df = CsvFile(full_file_name_list[0]).get_dataframe_windows_encoding_with_chunking()
        self.assertEqual(1, df.get_chunk().shape[0])
        UtilForTesting.file_teardown(temp_dir)

    def test_get_dataframe_utf8_encoding_with_header_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_1', content="col1,col2\ntest1,test2")
        df = CsvFile(full_file_name_list[0]).get_dataframe_utf8_encoding_with_header()
        self.assertEqual(df.shape[0], 1)
        self.assertEqual(df.columns[0], 'col1')
        self.assertEqual(df.columns[1], 'col2')

        UtilForTesting.file_teardown(temp_dir)

    def test_get_dataframe_utf8_encoding_without_header_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('test_df_1', content="test1,test2")
        df = CsvFile(full_file_name_list[0]).get_dataframe_utf8_encoding_without_header(['col1', 'col2'])
        self.assertEqual(df.shape[0], 1)
        self.assertEqual(df.columns[0], 'col1')
        self.assertEqual(df.columns[1], 'col2')
        UtilForTesting.file_teardown(temp_dir)

    #################################################
    # Test file_manager.file inherited functionality
    #################################################
    def test_file_name_property(self):
        self.assertEqual(CsvFile("c:\\temp\\test.csv").file_name, "test.csv")

    def test_full_file_name_property(self):
        self.assertEqual(CsvFile("c:\\temp\\test.csv").full_file_name, "c:\\temp\\test.csv")

    def test_full_extension_property(self):
        self.assertEqual(CsvFile("c:\\temp\\test.csv").extension, "csv")

    def test_mark_file_as_done_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('done')

        CsvFile(full_file_name=full_file_name_list[0]).mark_as_done()
        self.assertEqual(os.path.exists(full_file_name_list[0] + ".done"), True)

        UtilForTesting.file_teardown(temp_dir)

    def test_mark_file_as_fail_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('fail')

        CsvFile(full_file_name=full_file_name_list[0]).mark_as_fail()
        self.assertEqual(os.path.exists(full_file_name_list[0] + ".fail"), True)

        UtilForTesting.file_teardown(temp_dir)

    def test_mark_file_as_nodata_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('nodata')

        CsvFile(full_file_name=full_file_name_list[0]).mark_as_nodata()
        self.assertEqual(os.path.exists(full_file_name_list[0] + ".nodata"), True)

        UtilForTesting.file_teardown(temp_dir)

    def test_get_total_line_count_for_file_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup(sub_directory="file_info", content="1\n2\n")

        self.assertEqual(
            CsvFile(full_file_name_list[0]).get_total_line_count_for_file(),
            2
        )
        UtilForTesting.file_teardown(temp_dir)

    def test_mark_file_as_ready_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('ready', '.done')

        result = CsvFile(full_file_name_list[0]).mark_file_as_ready()

        self.assertEqual(result, True)
        self.assertEqual(os.path.exists(os.path.splitext(full_file_name_list[0])[0]), True)

        UtilForTesting.file_teardown(temp_dir)

    def test_to_sql_server_method(self):
        temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('ready', '', content="seconds,minutes\n3600,12\n")

        csv_file = CsvFile(full_file_name_list[0])
        return_df = CsvFile(full_file_name_list[0]).to_sql_server(
            pandas_dataframe=csv_file.get_dataframe_windows_encoding_with_chunking(batch_size=2),
            sql_configuration=SqlConfiguration(
                server_type="microsoft",
                server_name="localhost\\sqlexpress",
                database_name="master",
                schema_name="dbo",
                table_name="staging_test_to_sql_server"
            )
        )

        self.assertEqual(["SECONDS", "MINUTES"], return_df.columns.to_list())
        UtilForTesting.file_teardown(temp_dir)

    # This will require some more work.
    # Todo: Complete this.
    # def test_to_sql_server_with_chunking_method(self):
    #     temp_dir, file_dir, full_file_name_list = UtilForTesting.file_setup('ready', '',
    #                                                                         content="seconds,minutes\n3600,12\n3600,12\n3600,12\n3600,12\n3600,12\n")
    #
    #     csv_file = CsvFile(full_file_name_list[0])
    #     return_df = CsvFile(full_file_name_list[0]).to_sql_server(
    #         pandas_dataframe=csv_file.get_dataframe_utf8_encoding_with_header(),
    #         sql_configuration=SqlConfiguration(
    #             server_type="microsoft",
    #             server_name="localhost\\sqlexpress",
    #             database_name="master",
    #             schema_name="dbo",
    #             table_name="staging_test_to_sql_server"
    #         )
    #     )
    #
    #     # self.assertEqual(["SECONDS", "MINUTES"], return_df.columns.to_list())
    #     UtilForTesting.file_teardown(temp_dir)


if __name__ == '__main__':
    unittest.main()
