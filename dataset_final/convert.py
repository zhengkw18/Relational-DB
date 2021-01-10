path_in = 'lineitem_old.tbl'
path_out = 'lineitem.tbl'
file_in = open(path_in)
file_out = open(path_out, 'a+')
cols = [10, 11, 12]
for line in file_in:
    str_list = line.split('|')
    for col in cols:
        date = str_list[col]
        if '-' in date:
            date_list = date.split('-')
        else:
            date_list = date.split('/')
        if len(date_list[1]) == 1:
            date_list[1] = '0' + date_list[1]
        if len(date_list[2]) == 1:
            date_list[2] = '0' + date_list[2]
        date = '-'.join(date_list)
        str_list[col] = date
    newline = '|'.join(str_list)
    file_out.write(newline)
