import math
import copy
import random
import csv
import os


# splitting data set into 80:20 ratio for training and testing respectively
def data_split(data_filename):
    temp_copy = copy.deepcopy(data_filename)
    size = len(temp_copy)
    training_data = []
    while len(training_data) < int(0.8 * size):
        i = random.randrange(len(temp_copy))
        training_data.append(temp_copy.pop(i))

    return [training_data, temp_copy]


def is_digit(n):
    try:
        int(n)
        return True
    except ValueError:
        return False


def create_test_data(files):
    with open("D:\Manpreet\sem4\MR\Project\data_preprocessing\output\\test.csv", "w",
              newline='') as result:
        wtr = csv.writer(result)
        wtr.writerow(("FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME", "CRS_ARR_TIME", "DEP_TIME",
                      "ARR_TIME", "DEP_DELAY", "ARR_DELAY"))

        for f in files:
            print(f)
            with open("C:\\Users\Manpreet\Downloads\AirOnTimeCSV\AirOnTimeCSV\\" + f, "r") as file:
                rdr = csv.reader(file)
                i = 1

                for r in rdr:

                    if i == 1:
                        i += 1
                        continue

                    date = r[4]
                    origin = r[8]
                    dest = r[11]
                    crs_dep_time = r[14]
                    dep_time = r[15]
                    dep_delay = r[16]
                    crs_arr_time = r[24]
                    arr_time = r[25]
                    arr_delay = r[26]

                    if date == '' or origin == '' or dest == '' or crs_dep_time == '' or crs_arr_time == '' or\
                            dep_time == '' or dep_delay == '' or arr_time == '' or arr_delay == '':
                        continue

                    wtr.writerow((date, origin, dest, crs_dep_time, crs_arr_time, dep_delay, arr_time, dep_delay, arr_delay))


def create_training_data(files):
    with open("D:\Manpreet\sem4\MR\Project\data_preprocessing\output\\training.csv", "w",
              newline='') as result:
        wtr = csv.writer(result)
        wtr.writerow(("FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME", "CRS_ARR_TIME", "DEP_TIME",
                      "ARR_TIME", "DEP_DELAY", "ARR_DELAY", "DELAY_LABLE"))

        for f in files:
            print(f)
            with open("C:\\Users\Manpreet\Downloads\AirOnTimeCSV\AirOnTimeCSV\\" + f, "r") as file:
                rdr = csv.reader(file)
                i = 1

                for r in rdr:

                    if i == 1:
                        i += 1
                        continue

                    date = r[4]
                    origin = r[8]
                    dest = r[11]
                    crs_dep_time = r[14]
                    dep_time = r[15]
                    dep_delay = r[16]
                    dep_delay_new = r[17]
                    crs_arr_time = r[24]
                    arr_time = r[25]
                    arr_delay = r[26]
                    arr_delay_new = r[27]
                    delay_label = 1

                    if is_digit(dep_delay_new) and is_digit(arr_delay_new):
                        if int(dep_delay_new) > 0:
                            delay_label = dep_delay_new
                        elif int(arr_delay_new) > 0:
                            delay_label = arr_delay_new
                        else:
                            delay_label = 0

                    if date == '' or origin == '' or dest == '' or crs_dep_time == '' or crs_arr_time == '' or\
                            dep_time == '' or dep_delay == '' or arr_time == '' or arr_delay == '':
                        continue

                    wtr.writerow((date, origin, dest, crs_dep_time, crs_arr_time, dep_delay, arr_time,
                                  dep_delay, arr_delay, delay_label))


def main():
    files = os.listdir("C:\\Users\Manpreet\Downloads\AirOnTimeCSV\AirOnTimeCSV")

    training_files, testing_files = data_split(files)
    create_test_data(testing_files)
    create_training_data(training_files)


main()
