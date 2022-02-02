# Utility functions are stored in here ofr clearer code structure
import numpy
import openpyxl
import pandas
from multiprocessing import Queue
import multiprocessing



def write_to_csv(path, csv_row):  # Write output files from Source_2
    with open(path, 'a', newline='', encoding="utf-8") as f:
        f.write(csv_row)

def csv2queue(file_path, q):
    try:
        print(file_path)
        dataList = pandas.read_csv(file_path, index_col=False, header=None)
        data_arr = numpy.array(dataList.values)
        for i in range(len(data_arr)):
            q.put(data_arr[i][0])
    except Exception as e:
        print(e)

# Converts an xlsx file to csv
def xlsx2csv(input_file, output_file):
    try:
        output_file = input_file[:-5] + ".csv"
        output_file = output_file.replace("Input", "Output")
        print(output_file)
        if(input_file[-5:] == '.xlsx'):
            wb_obj = openpyxl.load_workbook(input_file)
            sheet = wb_obj.active
            for row in sheet.iter_rows(max_row=sheet.max_row):
                temp = ""
                row_curr = "\""
                cell = 0
                for cell in range(sheet.max_column):
                    temp = str(row[cell].value)
                    temp = temp.replace("\"", "\"\"")
                    row_curr += temp + "\",\""
                    cell +=1
                write_to_csv(output_file, (row_curr[:-2] + "\n"))
        else:
            raise Exception("File extension is wrong")
    except Exception as e:
        print(e)

# Takes file1 csv, takes file2 csv
# Compare two files by their given column indexes (file1_p, file2_p)
# Creates an output file according to the difference between two files from file1
def compare2diff(input1, input2, output):
    try:
        q = Queue()
        create_queue_infile(input1, q)
        pool = multiprocessing.Pool(processes=(multiprocessing.cpu_count() - 1))
        while not (q.empty()):
            res = pool.apply_async(substract_from_file, args=(q.get(), input2, 5, output,))
        pool.close()
        pool.join()
    except Exception as e:
        print(e)

# Creates a queue from the rows of a given csv file
# path -> path to csv file, q -> queue
def create_queue_infile(path, q):
    try:
        dataList = pandas.read_csv(path, index_col=False, header=None)
        data_arr = numpy.array(dataList.values)
        for i in range(len(data_arr)):
            q.put(str(data_arr[i][3]) + " " + str(data_arr[i][4]) + " " + str(data_arr[i][5]))
    except Exception as e:
        print(e)

# Search for a given field in a given file with given column index
# Write to another file if not found
def substract_from_file(field, file, col_index, output):
    try:
        found = False
        dataList = pandas.read_csv(file, low_memory=False, index_col=False, header=None)
        data_arr = numpy.array(dataList.values)
        for i in range(len(data_arr)):
            if ( transliterate_to_en(str(field).upper()) in transliterate_to_en(str(data_arr[i][col_index]).upper()) ) or ( transliterate_to_en_v2(str(field).upper()) in transliterate_to_en_v2(str(data_arr[i][col_index]).upper()) ):
                found = True
        if not found:
            write_to_csv(output, str(field + "\n").upper())

    except Exception as e:
        print(e)

def transliterate_to_en (string):
    ret = string
    ret = ret.replace('Ç','C')
    ret = ret.replace('ç','c')
    ret = ret.replace('İ','I')
    ret = ret.replace('i','i')
    ret = ret.replace('I','I')
    ret = ret.replace('ı','i')
    ret = ret.replace('Ğ','G')
    ret = ret.replace('ğ','g')
    ret = ret.replace('Ö','O')
    ret = ret.replace('ö','o')
    ret = ret.replace('Ş','S')
    ret = ret.replace('ş','s')
    ret = ret.replace('Ü','U')
    ret = ret.replace('ü','u')
    ret = ret.replace('Ə','E')
    ret = ret.replace('ə','e')
    return str(ret).upper()

def transliterate_to_en_v2 (string):
    ret = string
    ret = ret.replace('Ç','C')
    ret = ret.replace('ç','c')
    ret = ret.replace('İ','I')
    ret = ret.replace('i','i')
    ret = ret.replace('I','I')
    ret = ret.replace('ı','i')
    ret = ret.replace('Ğ','G')
    ret = ret.replace('ğ','g')
    ret = ret.replace('Ö','O')
    ret = ret.replace('ö','o')
    ret = ret.replace('Ş','S')
    ret = ret.replace('ş','s')
    ret = ret.replace('Ü','U')
    ret = ret.replace('ü','u')
    ret = ret.replace('Ə','A')
    ret = ret.replace('ə','a')
    return str(ret).upper()
