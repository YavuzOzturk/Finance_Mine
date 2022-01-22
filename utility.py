# Utility functions are stored in here ofr clearer code structure
import openpyxl

def write_to_csv(path, csv_row):  # Write output files from Source_2
    with open(path, 'a', newline='', encoding="utf-8") as f:
        f.write(csv_row)

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
                    row_curr += temp + "\",\""
                    cell +=1
                write_to_csv(output_file, (row_curr[:-2] + "\n"))

        else:
            raise Exception("File extension is wrong")
    except Exception as e:
        print(e)