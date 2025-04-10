import mysql.connector
import pandas as pd
import csv
import itertools

# STEP 1
filename = input("What is the name of the csv you'd like to input? ")

df = pd.read_csv(filename)

# Return the rows and columns
dimen = df.shape
print(f"The dimensions are as follows: {dimen}")

# Sample records
sample = df.head()
print("Here are some samples of the data")
print(sample)

# data types 
types = df.dtypes
print(" ")
print("Here are the data types present.")
print(types)
print(" ")

func_dep = input("Enter the functional dependencies for the dataset, in a A → B format, where each dependency is / separated. ")
prim_key = input("Enter the primary key(s), comma separated. ")

# Separates string input
func_dep = func_dep.split("/")

# Obtains all column names/attributes
all_attributes = df.columns.tolist()

lhs_list = []
rhs_list = []

# For loop used to separate the list into left hand and right hand sides.
for att in func_dep:
    sep_dep = att.split("→")
    lhs, rhs = sep_dep
    lhs_list.append(lhs)
    rhs_list.append(rhs)

# Function that checks that accounts for multiple attributes

def check_list(raw_list):
    for att in raw_list:
        if "," in att:
            new_att = att.split(",")
            raw_list.remove(att)
            for new in new_att:
                raw_list.append(new)
    return raw_list
lhs_list = check_list(lhs_list)
rhs_list = check_list(rhs_list)

# STEP 2
def find_cand_keys(lhs_list, rhs_list, all_attributes):
    '''
    Takes in lhs_list(list of the left side of fds), rhs_list, and all_attributes(list of
    all the attributes in the df), and uses them to calculate the candidate keys
    '''
    # Make every item in the lists the same type
    for i in range(len(lhs_list)):
        if type(lhs_list[i]) == list:
            continue
        else:
            lhs_list[i] = [lhs_list[i]]
    for i in range(len(rhs_list)):
        if type(rhs_list[i]) == list:
            continue
        else:
            rhs_list[i] = [rhs_list[i]]
    # Flatten the lists
    lhs_comp = []
    for lis in lhs_list:
        for item in lis:
            if item not in lhs_comp:
                lhs_comp.append(item)
            else:
                continue
    rhs_comp = []
    for lis in rhs_list:
        for item in lis:
            if item not in rhs_comp:
                rhs_comp.append(item)
            else:
                continue
    # Categorize
    neither = []
    for att in all_attributes:
        if att not in lhs_comp and att not in rhs_comp:
            neither.append(att)
    
    rhs_only = []
    for att in all_attributes:
        if att not in lhs_comp:
            rhs_only.append(att)
    
    lhs_only = []
    for att in all_attributes:
        if att not in rhs_comp:
            lhs_only.append(att)
    
    necessary = lhs_only + neither
    other = [a for a in all_attributes if a not in necessary]
    
    candidate_keys = []
    
    for r in range(len(other) + 1):
        for combo in itertools.combinations(other, r):
            trial_key = necessary + list(combo)
            
            # compute closure of trial_key
            closure = trial_key[:]  # start with the key itself
            while True:
                b_len = len(closure)
                # one pass through all FDs
                for j in range(len(lhs_list)):
                    lhs = lhs_list[j]
                    rhs = rhs_list[j]
                    ok = True
                    for x in lhs:
                        if x not in closure:
                            ok = False
                            break
                    if ok:
                        # add any new rhs attrs
                        for y in rhs:
                            if y not in closure:
                                closure.append(y)
                # stop once no new attrs were added
                if len(closure) == b_len:
                    break
            
            #Does closure contain every attribute
            covers_all = True
            for a in all_attributes:
                if a not in closure:
                    covers_all = False
                    break
            if not covers_all:
                continue
            
            # No existing key is a subset of trial_key
            minimal = True
            for existing in candidate_keys:
                is_subset = True
                for x in existing:
                    if x not in trial_key:
                        is_subset = False
                        break
                if is_subset:
                    minimal = False
                    break
            
            if minimal:
                candidate_keys.append(trial_key)
    
    return candidate_keys
    
print(" ")    
candidate_keys = find_cand_keys(lhs_list, rhs_list, all_attributes)
print(candidate_keys)
print(" ")
def compute_closures(candidate_keys, lhs_list, rhs_list):
    '''
    Takes in the list of candidate keys, lhs, and rhs, and
    computese the attribute closures
    '''
    
    all_sets = {}
    
    # make all values a list to avoid confusion
    for k in candidate_keys:
        if type(k) == list:
            closures= k[:]
        else:
            closures = [k]
    #now we begin to  make the closure sets
        appended = True
        # While loop to make sure everything is iterated through
        while appended:
            # Flag used to make program loop through everything
            # as well as make the while loop possible
            appended = False
            for i in range(len(lhs_list)):
                lhs_dep = lhs_list[i]
                att_in = True
                for att in lhs_dep:
                    if att not in closures:
                        att_in = False
                        break
                if att_in:
                    for att in rhs_list[i]:
                        if att not in closures:
                            closures.append(att)
                            appended = True
        if type(k) == list:
            all_sets[tuple(k)] = closures
        else:
            all_sets[k] = closures
    return all_sets
print(" ")
print(compute_closures(candidate_keys, lhs_list, rhs_list))
print(" ")
def partial_dep(candidate_keys, lhs_list, rhs_list):
    '''
    Finds the partial dependencies based upon the candidate keys.
    Partial dependencies occur when a non candidate key is
    dependent partly on one.
    '''
    
    partial_deps = []
    # make each key a list to make sure no errors occur
    for key in candidate_keys:
        if type(key) == list:
            keys = key[:]
        else:
            keys = [key]
    
        if len(keys) < 2:
                continue
        for i in range(len(lhs_list)):
                lhs = lhs_list[i]
                rhs = rhs_list[i]
                if len(lhs) >= len(keys):
                    continue
                for att in lhs:
                    if att not in keys:
                        break
                    else:
                        for att in rhs:
                            if att not in keys:
                                partial_deps.append((lhs, rhs, keys))
                                break
    return partial_deps
print(" ")
partial_deps = partial_dep(candidate_keys, lhs_list, rhs_list)
print(f"These are the partial dependencies: {partial_deps}")
print(" ")

def transitive_dep(candidate_keys, lhs_list, rhs_list):
    transitive_deps = []
    # Gather a list of all the attributes in the fd list
    all_attrs = []
    for lhs in lhs_list:
        for att in lhs:
            if att not in all_attrs:
                all_attrs.append(att)
    for rhs in rhs_list:
        for att in rhs:
            if att not in all_attrs:
                all_attrs.append(att)
    
    # Find out which attributes appear in a candidate key
    cand_attrs = []
    for key in candidate_keys:
        if type(key) == list:
            for att in key:
                if att not in cand_attrs:
                    cand_attrs.append(att)
        else:
            if key not in cand_attrs:
                cand_attrs.append(key)
    # Use this to find which are not in candidate_keys
    not_cand = []
    for att in all_attrs:
        if att not in cand_attrs:
            not_cand.append(att)
    # if all lhs and at least one rhs
    # is in not_cand,  it is transitive
    for i in range(len(lhs_list)):
        lhs = lhs_list[i]
        rhs = rhs_list[i]
        for item in lhs:
            if item not in not_cand:
                break
        else:
            for it in rhs:
                if it not in not_cand:
                    transitive_deps.append((lhs, rhs))
    
    return transitive_deps
print(" ")    
print(transitive_dep(candidate_keys, lhs_list, rhs_list))
print(" ")
# STEP 3

def one_nf(df):
    # checks if there are multiple values in a single entry
    
    for col in df.columns:
        for val in df[col]:
            if "," in str(val):
                return False
    
    return True
print(" ")
is_onenf = one_nf(df)
print(f"True if it is in 1NF, False if it is not: {is_onenf}")
print(" ")
def two_nf(df, primary_keys, partial_deps):
    # Brings the df to 2NF form
    if type(primary_keys) == str:
        primary_keys = [ primary_keys.split(",") ]
    df = df.drop_duplicates()

    relations = []
    pulled    = []

   
    for pk in primary_keys:
        # look at each partial FD
        for lhs, rhs, key_attrs in partial_deps:
            # only handle those partials whose key_attrs exactly match this pk
            if len(key_attrs) != len(pk):
                continue
            match = True
            for a in key_attrs:
                if a not in pk:
                    match = False
                    break
            if not match:
                continue

            # build the new relation on A u B
            attrs = lhs[:]    
            for b in rhs:
                if b not in attrs:
                    attrs.append(b)
                if b not in pk and b not in pulled:
                    pulled.append(b)

            # project and store
            rel = df[attrs].drop_duplicates().reset_index(drop=True)
            relations.append((attrs, rel))

    # build the “remainder” relation from the first primary key
    base_pk = primary_keys[0]
    remainder_attrs = base_pk[:]  # start with the PK
    for col in df.columns.tolist():
        if col not in base_pk and col not in pulled:
            remainder_attrs.append(col)
    remainder = df[remainder_attrs].drop_duplicates().reset_index(drop=True)

    # 6) Return remainder first, then each partial‑dep table
    return [(remainder_attrs, remainder)] + relations
two_nf_done = two_nf(df, prim_key, partial_deps)

# STEP 4:

mydbase = mysql.connector.connect(host='localhost', user="root", passwd="VsK1vDOp@1@1")
mycursor = mydbase.cursor()

mycursor.execute("DROP DATABASE IF EXISTS student;")

mycursor.execute("CREATE DATABASE student;")

mycursor.execute("USE student;")

counter = 0
for rel in two_nf_done:
    att = rel[0]
    relation = rel[1]
    
    name = "table " + str(counter)
    counter += 1
    
    # prevents error from table already existing
    mycursor.execute(f"DROP TABLE IF EXISTS `{name}`;")
    
    cols = ""
    
    for col in att:
        cols += "`" + col + "` VARCHAR(255) NOT NULL, "

    create_sql = (
        "CREATE TABLE `" + name + "` ("
        " `id` INT UNSIGNED NOT NULL AUTO_INCREMENT, "
        + cols +
        " PRIMARY KEY(`id`)"
        ") ENGINE=InnoDB;"
    )
    mycursor.execute(create_sql)
    
    cols_list = ""
    placeholders = ""
    for col in att:
        if cols_list:
            cols_list += ", "
            placeholders += ", "
        cols_list += "`" + col + "`"
        placeholders += "%s"

    insert_sql = (
        "INSERT INTO `" + name + "` (" + cols_list + ") "
        "VALUES (" + placeholders + ");"
    )

    # insert each row
    for row in relation.itertuples(index=False):
        # row is a tuple of values in the same order as attrs
        mycursor.execute(insert_sql, tuple(row))

    print("Populated", name, "with", len(relation), "rows.")
    
    mycursor.execute("SELECT * FROM `" + name + "`;")
    rows = mycursor.fetchall()   # list of tuples, one per row


    cols = [col[0] for col in mycursor.description]
    print("\t".join(cols))

    # print each row
    for row in rows:
        # convert each value to string and join with tabs
        # So it is not cluttered
        print("\t".join(str(v) for v in row))
    
    # select all rows
    mycursor.execute(f"SELECT * FROM `{name}`;")
    rows = mycursor.fetchall()

# STEP 5
user_cmd = input("Enter in a valid SQL command. To exit the program, type Exit. ")
while True:
    if user_cmd == "Exit":
        break
    else:
        try:
            mycursor.execute(user_cmd)
        except Exception as e:
            print(f"There was an error. This is what it was: {e} ")
            break

    if "SELECT" in user_cmd:
        rows = mycursor.fetchall()
        for row in rows:
            print(row)
    else:
        # commit other changes
        mydbase.commit()

mydbase.commit()
mydbase.close()
