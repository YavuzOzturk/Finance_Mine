# Utility functions are stored in here ofr clearer code structure
import numpy
import openpyxl
import pandas


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
def compare2diff(file1_row, file1_p, file2, file2_p, output):
    print(file1)
    print(file1_p)
    print(file2)
    print(file2_p)
    print(output)
