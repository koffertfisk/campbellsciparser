import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from campbellsciparser import device

if __name__ == '__main__':
    file = os.path.join('D:/virtualenv/campbellsciparser/campbellsciparser/tests/testdata/csv_testdata_10_rows.dat')
    output_file = os.path.join('d:/testnew/test.dat')
    data_split_unfiltered = device.CR10X.read_array_ids_data(infile_path=file)
    #data_split_filtered = device.CR10X.filter_data_by_array_ids('201', data=data_split_unfiltered)
    #data_split_array_id_filtered = data_split_filtered.get('201')
    headers_1 = ["Array_Id","Year_RTM","Day_RTM","Hour_Minute_RTM","Tot_rad_AVG","Air_Temp2_AVG","Air_Temp1",
                     "Humidity_AVG","Wind_spd_S_WVT","Wind_spd_U_WVT","Wind_dir_DU_WVT","Wind_dir_SDU_WVT",
                     "Wind_spd3_AVG","BadTemp_AVG","PAR_AVG","Air_Pres_AVG"]
    device.CR10X.export_array_ids_to_csv(data=data_split_unfiltered, array_ids_info={'201': {'file_path': output_file, 'headers': headers_1}}, match_num_of_columns=False)
    #data_exported_file = device.CR10X.read_mixed_data(infile_path=output_file)
