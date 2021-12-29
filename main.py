import multiprocessing
from multiprocessing import Queue
from os import walk
import openpyxl as openpyxl
from datetime import datetime
import pandas
import numpy
import time
import xlrd as xlrd

output_folder = 'Invoice/Output/'
data_folder = 'Invoice/'

def create_cross_ref(arg1, arg2): # arg1: dates, arg2: invoice files
    i = 0

    for file in arg2:
        path_to_csv = 'Invoice/' + file + '.csv'
        csvFile = pandas.read_csv(path_to_csv)
        # csvFile['G_tarix'] = pandas.to_datetime(csvFile['G_tarix'], format='%Y%m%d', errors='coerce')
        arr = numpy.array(csvFile.values)
        for row in range(len(arr)):
            if arr[row][2] != 'None' and float.is_integer(arr[row][2]) :
                date1 = xlrd.xldate_as_datetime(arr[row][2],0)
                date2 = date1.date()
                date3 = date2.isoformat()
            else :
                print("Else")

            #else:
                #if file[-4:-1] == "ymd":
                #    print('ymd')
            #print(row)
            #if file[:-5] == ')':
            #    print(file)


def write_csv(path, csv_row):
    full_path = output_folder + path
    with open(full_path, 'a', newline='', encoding="utf-8") as f:
        f.write(csv_row)


def create_date_dist (argument1):
    print(argument1)
    wb_obj = openpyxl.load_workbook(data_folder+argument1)
    sheet = wb_obj.active
    date = ""
    row_curr = ""
    for row in sheet.iter_rows(max_row=sheet.max_row):
        if row[1].value != "Tarix":
            date = str(row[1].value)
            date = date.split(' ')[0]
            date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d')
            row_curr = "\""+str(date) +"\",\""+ str(row[3].value) +"\",\""+ str(row[4].value) +"\",\""+ str(row[6].value) +"\",\""+ str(row[8].value) + "\"\n"
            print(row_curr)
            write_csv(date+'.csv', row_curr)
            date = ""
            row_curr = ""


def create_queue(path, q):
    filenames_s = next(walk(path), (None, None, []))[2]
    for file in filenames_s:
        q.put(file, True, None)

def create_list(path, l):
    filenames_s = next(walk(path), (None, None, []))[2]
    for file in filenames_s:
        l.append(file[:-4])

def main():
    start = time.time()
    #multiprocess addition
    q = Queue()
    file_list = list()
    procs = []
    create_queue('Output/', q)
    create_list(data_folder, file_list)
    file_list.sort()
    pool = multiprocessing.Pool(processes=(multiprocessing.cpu_count()-1))
    while not (q.empty()):

        # proc_f_1 = pool.map_async(search_student, q)
        # proc_f_1 = Process(target=read_xlsx, args=(path_src, q.get()))
        res = pool.apply_async(create_cross_ref, args=(q.get(), file_list,))

        # complete the processes
    pool.close()
    pool.join()
    print("Execution time : " , time.time() - start)


if __name__ == '__main__':
    main()


