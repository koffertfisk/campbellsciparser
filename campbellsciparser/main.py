import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if __name__ == '__main__':
    DATA_ROOT = 'C:/Users/Marcus/Desktop/data'
    fpath = os.path.join(DATA_ROOT, 'galtennew.dat')
    #fpath = os.path.join(DATA_ROOT, 'Island_Output5min.dat')
    fieldnames = ["Array_Id","Year_RTM","Day_RTM","Hour_Minute_RTM","Tot_rad_AVG","Air_Temp2_AVG","Air_Temp1",
                     "Humidity_AVG","Wind_spd_S_WVT","Wind_spd_U_WVT","Wind_dir_DU_WVT","Wind_dir_SDU_WVT",
                     "Wind_spd3_AVG","BadTemp_AVG","PAR_AVG","Air_Pres_AVG"]
    #data = CR10X.read_mixed_data(fpath, line_num=0, fix_floats=True)
    data = CR10X.read_array_ids_data(fpath, line_num=0, fix_floats=True)
    #data_filtered = CR10X.filter_array_ids(data, '201')
    #fieldnames_indexes = [i for i, name in enumerate(fieldnames)]
    updated = CR10X.convert_time(data=data.get('201'), headers=fieldnames, time_columns=["Year_RTM","Day_RTM","Hour_Minute_RTM"], data_time_zone='Etc/GMT-1', to_utc=True)
    #headers_updated, data_converted = updated
    CR10X.export_to_csv(data_converted, 'D:/test/new/out.dat', header=headers_updated, output_mismatched_columns=True, include_time_zone=True)
