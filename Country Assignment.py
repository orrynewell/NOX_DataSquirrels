'''
Title       - Country Assignment Script
Description - This script will take in a footprint feature class and a country polygon file. From this it will append
              the appropriate country information to the footprint item.
Created By  - Orry Newell
Updates
    - 24MAR2020 - Script was created
    - 25MAR2020 - Border/Ocean options added
    - 26MAR2020 - Documentation added
'''
# Import Statments
import arcpy
from multiprocessing import Pool
from datetime import datetime
import os
import pprint
import unicodedata


# Adds a menu to the command line, this will later be replaced by radial buttons in an ArcTool.
def menu():
    user_decision = '0'
    while user_decision not in ['1', '2', '3']:
        print("\nThis process will append spatial information to the input feature class.\n"
              "Please select one of the following options.\n"
              "1 - Dominant Country Information is appended to the output feature class.\n"
              "2 - Water body or country information is appended to the output feature class.\n"
              "3 - If a border is detected the output will show as follows (USA-MEX Border)")
        user_decision = raw_input("Choice: ")
    return user_decision


# Creates a dictionary of country information in the following format
# {Country Name: "GENC3": GENC3,
#                "AOR": AOR}
def get_country_info(country):
    country_dict = {}
    with arcpy.da.SearchCursor(country, ["CNTRY_NAME", "GENC3", "AOR"]) as cursor:
        for row in cursor:
            if row[0] not in country_dict:
                country_dict[row[0]] = {"GENC3": row[1],
                                        "AOR": row[2]}
    return country_dict


# Same as country information, but with ocean data.
def get_ocean_info(ocean):
    ocean_dict = {}
    with arcpy.da.SearchCursor(ocean, ['Name1', 'AOR']) as cursor:
        for row in cursor:
            if row[0] not in ocean_dict:
                ocean_dict[row[0]] = {"AOR": row[1]}
    return ocean_dict


# Actually updates the dictionary infomration for gather values, reduces duplication of code.
def update_dict(ti, fp, fc, field, d):
    if arcpy.Exists(ti):
        arcpy.management.Delete(ti)
    ti = arcpy.analysis.TabulateIntersection(fp, "OBJECTID", fc, ti, field)
    with arcpy.da.SearchCursor(ti, ["OBJECTID_1", field, "PERCENTAGE"]) as cursor:
        for row in cursor:
            if row[0] not in d:
                d[row[0]] = {"Count": 1,
                            "Intersect": [row[1], round(row[2], 2)]}
            else:
                d[row[0]]["Count"] += 1
                d[row[0]]["Intersect"].extend([row[1], round(row[2], 2)])
    return d


# Creates a dictionary of the relationships between the overlap of the footprint and country polygons.
# Output dictionary entry looks as follows
# {OBJECTID: {"Count": Number of intersects,
#             "Intersects": [Country, Intersect Value]}}
# A future update may be taking in a ocean polygon and appending the sea names to the footprints instead of
# country names
def gather_values(fp, country, ocean):
    v_dict = {}
    gdb = r'D:\Telework\Task-TQUT\TaskDB.gdb'
    ti_name = gdb + '\\outTable'
    v_dict = (update_dict(ti_name, fp, country, "CNTRY_NAME", v_dict))
    if ocean is not None:
        v_dict = (update_dict(ti_name, fp, ocean, "NAME1", v_dict))
    return v_dict


# Creates the range that each core of the machine will process.
def partition_dict(value_dict, cores):
    out_list = []
    dict_length = len(value_dict)
    increment = dict_length / cores
    out_list.append(dict(value_dict.items()[:increment]))
    for i in range(1, cores):
        out_list.append(dict(value_dict.items()[increment * i: increment * (i + 1) + 1]))
    return out_list


# Creates a script for which overlap feature overlapped the most with the footprint
# Return looks something like this
# {OBJECTID: Country Value}
# Possible future update would be adding a parameter to show a boarder relationship, such as USA-MEX (Boarder)
def decision(v_dict):
    update_dict = {}
    for k, v in v_dict.items():
        if v['Count'] == 1:
            update_dict[k] = v["Intersect"][0]
        else:
            highest_per = 0
            for pos in range(1, len(v['Intersect']), 2):
                per_value = v['Intersect'][pos]
                if per_value > highest_per:
                    highest_per = per_value
                    highest_country = v['Intersect'][pos - 1]
            update_dict[k] = highest_country
    return update_dict


# Seperate decision function made for the border option since it concatenates two inputs and selects between them
def border_decision(v_dict):
    update_dict = {}
    for k, v in v_dict.items():
        if v['Count'] == 1:
            update_dict[k] = v["Intersect"][0]
        elif v['Count'] == 2:
            entry_list = sorted([v["Intersect"][0][:3], v["Intersect"][2][:3]])
            update_dict[k] = "-".join(entry_list) + " Border"
        else:
            highest_per, second_per = 0, 0
            highest_country, second_country = '', ''
            for pos in range(1, len(v['Intersect']), 2):
                per_value = v['Intersect'][pos]
                if per_value > highest_per:
                    second_per = highest_per
                    second_country = highest_country
                    highest_per = per_value
                    highest_country = v['Intersect'][pos - 1]
            try:
                entry_list = sorted([highest_country[:3], second_country[:3]])
            except TypeError:
                if type(highest_country) == unicode:
                    highest_country = unicodedata.normalize('NFKD', highest_country).encode('ascii', 'ignore')
                if type(second_country) == unicode:
                    second_country = unicodedata.normalize('NFKD', second_country).encode('ascii', 'ignore')
                entry_list = sorted([highest_country[:3], second_country[:3]])
            update_dict[k] = "-".join(entry_list) + " Border"
    return update_dict


# Assigns the country name, GENC3, and AOR to the footprint feature class
def assignment(fp, v_tu, c_dict, o_dict):
    row_count = 0
    total_rows = int(arcpy.management.GetCount(fp).getOutput(0))
    with arcpy.da.UpdateCursor(fp, ["OBJECTID", "Country", "GENC3", "AOR"]) as cursor:
        for row in cursor:
            row_count += 1
            if row[0] in v_tu.keys():
                country_name = v_tu[row[0]]
                row[1] = country_name
                if country_name in c_dict.keys():
                    row[2] = c_dict[country_name]["GENC3"]
                    row[3] = c_dict[country_name]["AOR"]
                elif country_name in o_dict.keys():
                    row[2] = 'N/A'
                    row[3] = o_dict[country_name]["AOR"]
                else:
                    row[2] = "N/A"
                    row[3] = "N/A"
                cursor.updateRow(row)
            if row_count % 1000 == 0:
                print("{} out of {} processed".format(row_count, total_rows))


# Main function calls all above functions
def main():
    footprint = r'D:\Telework\Task-TQUT\TaskDB.gdb\TestData'
    country_file = r'D:\Telework\HelpfulData.gdb\Countries_WGS84'
    ocean_file = r'D:\Telework\Task-TQUT\TaskDB.gdb\Ocean_d'
    user_decision = int(menu())
    print("")
    val_to_update = {}
    cores = 2
    print("Creating Country and Ocean Database")
    country_dict = get_country_info(country_file)
    ocean_dict = get_ocean_info(ocean_file)
    print("---Completed---\n")
    print("Gathering Values")
    if user_decision in [1, 3]:
        value_dict = gather_values(footprint, country_file, None)
    elif user_decision == 2:
        value_dict = gather_values(footprint, country_file, ocean_file)
    print("---Completed---\n")
    print("Partitioning Dictionary")
    partition_dict_list = partition_dict(value_dict, cores)
    print(len(partition_dict_list))
    print("---Completed---\n")
    print("Initializing Multiprocessing")
    p = Pool(cores)
    if user_decision in [1, 2]:
        temp = p.map(decision, partition_dict_list)
    elif user_decision == 3:
        for p in partition_dict_list:
            temp = border_decision(p)
            val_to_update.update(temp)
    print("---Completed---\n")
    print("Assigning Values")
    assignment(footprint, val_to_update, country_dict, ocean_dict)
    print("---Completed---\n")

# Click to run
if __name__ == "__main__":
    start_time = datetime.now()
    print"Start Time: {}".format(start_time)
    main()
    end_time = datetime.now() - start_time
    print"Total Time: {}".format(end_time)
