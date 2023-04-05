import random

def global_commit(list1, list2):
    if len(list1) != len(list2):
        raise ValueError("Input lists must have the same length.")

    if not list1 or not list2:
        raise ValueError("Input lists cannot be empty.")

    def count_values(key, lst):
        value_counts = {}
        
        for d in lst:
            if key in d:
                print(d)
                if d[key] in value_counts:
                    value_counts[d[key]] += 1
                else:
                    value_counts[d[key]] = 1
        return value_counts

    def find_max_value(counts):
        max_count = 0
        max_values = []
        for value, count in counts.items():
            if count > max_count:
                max_count = count
                max_values = [value]
            elif count == max_count:
                max_values.append(value)

        return max_values
    

    globaldata = {}
    all_keys = set(k for d in list1 for k in d)
    # i=0
    for key in all_keys:
        value_counts1 = count_values(key, list1)
        max_value1 = find_max_value(value_counts1)
        
      

        if len(max_value1) != 1:
            maxtimestamps= []
            for value in max_value1:
                indices = [i for i, d in enumerate(list1) if key in d and d[key] == value]
                result = [max(list2[i].get(key)) for i in indices]
                maxtimestamps.append(result,value)
            max_value1= max(maxtimestamps,key=lambda x: x[0])[1]

        globaldata[key] = max_value1[0]
        
    return globaldata