import random
class value:
    def __init__(self,t,d):
        self.times=t
        self.date=set([d])
with open('C:/Users/Tongji511/Desktop/distribute system/tb_call_201202_random.txt', 'r') as f:
    row_data = f.read().split('\n')
    list_data = [i.split('\t') for i in row_data]
    random.seed(1234)
    part_list_data = list_data
    name_dict={}
#print(name_dict[f3[0][1]])

    for j in part_list_data:
        if len(j)<2:
            continue
        if j[1] in name_dict:
            v = name_dict[j[1]]
            v.times+=1
            v.date.add(j[0])
            name_dict[j[1]]=v
        else:
            v = value(1,j[0])
            name_dict[j[1]]=v
    with open('C:/Users/Tongji511/Desktop/distribute system/answer1.txt', 'w') as f1:
        for (k, v) in name_dict.items():
            # print(k)#user
            date_length = len(v.date)
            average_times = v.times / date_length
            f1.write(k+','+str(average_times)+',' +'\n')