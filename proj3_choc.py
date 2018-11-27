import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
try:
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)
    
    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)
        
    statement = '''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' TEXT NOT NULL,
            'REF' TEXT NOT NULL,
            'ReviewDate' TEXT NOT NULL,
            'CocoaPercent' REAL NOT NULL,
            'CompanyLocationId' INTEGER NOT NULL,
            'Rating' REAL NOT NULL,
            'BeanType' TEXT,
            'BroadBeanOriginId' INTEGER
        );
    '''
    cur.execute(statement)
    statement = '''
        CREATE TABLE 'Countries' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Alpha2' TEXT NOT NULL,
                'Alpha3' TEXT NOT NULL,
                'EnglishName' TEXT NOT NULL,
                'Region' TEXT,
                'Subregion' TEXT,
                'Population' INTEGER NOT NULL,
                'Area' REAL
        );
    '''
    cur.execute(statement)
    conn.commit()
    
    with open(COUNTRIESJSON) as f:
        json_contents = f.read()
        reader = json.loads(json_contents)
        for row in reader:
            insertion = (None, row["alpha2Code"], row["alpha3Code"], row["name"], row["region"], row["subregion"], row["population"], row["area"])
            statement = 'INSERT INTO "Countries" '
            statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
            cur.execute(statement, insertion)
    conn.commit()
    
    with open(BARSCSV) as f:
        reader = csv.reader(f)
        for i,row in enumerate(reader):
            if (i>0):
                statement = 'SELECT Id FROM Countries WHERE EnglishName = "'
                statement += row[5]
                statement += '"'
                cur.execute(statement)
                result = cur.fetchall()
                CompanyLocationId = result[0][0]
                statement = 'SELECT Id FROM Countries WHERE EnglishName = "'
                statement += row[8]
                statement += '"'
                cur.execute(statement)
                result = cur.fetchall()
                try:
                    BroadBeanOriginId = result[0][0]
                except:
                    BroadBeanOriginId = None
                insertion = (None, row[0], row[1], row[2], row[3], row[4], CompanyLocationId, row[6], row[7], BroadBeanOriginId)
                statement = 'INSERT INTO "Bars" '
                statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                cur.execute(statement, insertion)
    
    conn.commit()
    conn.close()
except:
    print("fail!")


# Part 2: Implement logic to process user commands
def process_command(response):
    command = response.split(" ")
    if (command[0] == "bars"):
        try:
            conn = sqlite3.connect(DBNAME)
            cur = conn.cursor()
            simple_params = []
            params = {}
            for i,c in enumerate(command):
                if (i>0):
                    if "=" not in c:
                        simple_params.append(c)
                    else:
                        c_split = c.split("=")
                        params[c_split[0]] = c_split[1]
            if (len(simple_params)>1):
                fail()
            statement = 'SELECT B.SpecificBeanBarName, B.Company, CompanyL.EnglishName, B.Rating, B.CocoaPercent, Origin.EnglishName '
            statement += 'FROM Bars AS B\n'
            statement += 'LEFT JOIN Countries AS CompanyL\n'
            statement += '  ON B.CompanyLocationId = CompanyL.Id\n'
            statement += 'LEFT JOIN Countries AS Origin\n'
            statement += '  ON B.BroadBeanOriginId = Origin.Id\n'
            if (params.__contains__("sellcountry")):
                statement += 'WHERE CompanyL.Alpha2 = "' + params["sellcountry"] + '"\n'
            elif (params.__contains__("sourcecountry")):
                statement += 'WHERE Origin.Alpha2 = "' + params["sourcecountry"] + '"\n'
            elif (params.__contains__("sellregion")):
                statement += 'WHERE CompanyL.Region = "' + params["sellregion"] + '"\n'
            elif (params.__contains__("sourceregion")):
                statement += 'WHERE Origin.Region = "' + params["sourceregion"] + '"\n'
            else:
                pass
            if ("cocoa" in simple_params):
                statement += 'ORDER BY CAST(B.CocoaPercent as DECIMAL) '
            elif ("ratings" in simple_params):
                statement += 'ORDER BY B.Rating '
            elif (not simple_params):
                statement += 'ORDER BY B.Rating '
            else:
                fail()
            if (params.__contains__("bottom")):
                statement += 'ASC\nLIMIT ' + str(params["bottom"])
            elif (params.__contains__("top")):
                statement += 'DESC\nLIMIT ' + str(params["top"])
            else:
                statement += 'DESC\nLIMIT 10'
            cur.execute(statement)
            result = cur.fetchall()
            conn.close()
            return result
        except:
            return []
    if (command[0] == "companies"):
        try:
            conn = sqlite3.connect(DBNAME)
            cur = conn.cursor()
            simple_params = []
            params = {}
            for i,c in enumerate(command):
                if (i>0):
                    if "=" not in c:
                        simple_params.append(c)
                    else:
                        c_split = c.split("=")
                        params[c_split[0]] = c_split[1]
            if (len(simple_params)>1):
                fail()
            statement = 'SELECT B.Company, CompanyL.EnglishName, '
            if ("cocoa" in simple_params):
                var_s = 'AVG(B.CocoaPercent) '
                statement += var_s
            elif ("bars_sold" in simple_params):
                var_s = 'COUNT(*) '
                statement += var_s
            elif ("ratings"in simple_params ):
                var_s = 'AVG(B.Rating) '
                statement += var_s
            elif (not simple_params):
                var_s = 'AVG(B.Rating) '
                statement += var_s
            else:
                fail()
            statement += 'FROM Bars AS B\n'
            statement += 'LEFT JOIN Countries AS CompanyL\n'
            statement += '  ON B.CompanyLocationId = CompanyL.Id\n'
            if (params.__contains__("country")):
                statement += 'WHERE CompanyL.Alpha2 = "' + params["country"] + '"\n'
            elif (params.__contains__("region")):
                statement += 'WHERE CompanyL.Region = "' + params["region"] + '"\n'
            else:
                pass
            statement += 'GROUP BY B.Company\n'
            statement += 'HAVING COUNT(*) > 4\n'
            statement += 'ORDER BY '+var_s
            if (params.__contains__("bottom")):
                statement += 'ASC\nLIMIT ' + str(params["bottom"])
            elif (params.__contains__("top")):
                statement += 'DESC\nLIMIT ' + str(params["top"])
            else:
                statement += 'DESC\nLIMIT 10'
            cur.execute(statement)
            result = cur.fetchall()
            conn.close()
            return result
        except:
            return []
    if (command[0] == "countries"):
        try:
            conn = sqlite3.connect(DBNAME)
            cur = conn.cursor()
            simple_params = []
            params = {}
            for i,c in enumerate(command):
                if (i>0):
                    if "=" not in c:
                        simple_params.append(c)
                    else:
                        c_split = c.split("=")
                        params[c_split[0]] = c_split[1]
            if (len(simple_params)>2):
                fail()
            statement = 'SELECT C.EnglishName, C.Region, '
            has_sort = True
            if ("cocoa" in simple_params):
                var_s = 'AVG(B.CocoaPercent) '
                statement += var_s
            elif ("bars_sold" in simple_params):
                var_s = 'COUNT(*) '
                statement += var_s
            elif ("ratings"in simple_params ):
                var_s = 'AVG(B.Rating) '
                statement += var_s
            elif (len(simple_params)<2):
                var_s = 'AVG(B.Rating) '
                statement += var_s
                has_sort = False
            else:
                fail()
            statement += 'FROM Bars AS B\n'
            statement += 'JOIN Countries AS C\n'
            if ("sellers" in simple_params):
                statement += '  ON B.CompanyLocationId = C.Id\n'
            elif ("sources" in simple_params):
                statement += '  ON B.BroadBeanOriginId = C.Id\n'
            elif (has_sort):
                if (len(simple_params)<2):
                    statement += '  ON B.CompanyLocationId = C.Id\n'
                else:
                    fail()
            else:
                if (len(simple_params)<1):
                    statement += '  ON B.CompanyLocationId = C.Id\n'
                else:
                    fail()
            if (params.__contains__("region")):
                statement += 'WHERE C.Region = "' + params["region"] + '"\n'
            else:
                pass
            statement += 'GROUP BY C.Id\n'
            statement += 'HAVING COUNT(*) > 4\n'
            statement += 'ORDER BY '+var_s
            if (params.__contains__("bottom")):
                statement += 'ASC\nLIMIT ' + str(params["bottom"])
            elif (params.__contains__("top")):
                statement += 'DESC\nLIMIT ' + str(params["top"])
            else:
                statement += 'DESC\nLIMIT 10'
            cur.execute(statement)
            result = cur.fetchall()
            conn.close()
            return result
        except:
            return []
    if (command[0] == "regions"):
        try:
            conn = sqlite3.connect(DBNAME)
            cur = conn.cursor()
            simple_params = []
            params = {}
            for i,c in enumerate(command):
                if (i>0):
                    if "=" not in c:
                        simple_params.append(c)
                    else:
                        c_split = c.split("=")
                        params[c_split[0]] = c_split[1]
            if (len(simple_params)>2):
                fail()
            statement = 'SELECT C.Region, '
            has_sort = True
            if ("cocoa" in simple_params):
                var_s = 'AVG(B.CocoaPercent) '
                statement += var_s
            elif ("bars_sold" in simple_params):
                var_s = 'COUNT(*) '
                statement += var_s
            elif ("ratings"in simple_params ):
                var_s = 'AVG(B.Rating) '
                statement += var_s
            elif (len(simple_params)<2):
                var_s = 'AVG(B.Rating) '
                statement += var_s
                has_sort = False
            else:
                fail()
            statement += 'FROM Bars AS B\n'
            statement += 'JOIN Countries AS C\n'
            if ("sellers" in simple_params):
                statement += '  ON B.CompanyLocationId = C.Id\n'
            elif ("sources" in simple_params):
                statement += '  ON B.BroadBeanOriginId = C.Id\n'
            elif (has_sort):
                if (len(simple_params)<2):
                    statement += '  ON B.CompanyLocationId = C.Id\n'
                else:
                    fail()
            else:
                if (len(simple_params)<1):
                    statement += '  ON B.CompanyLocationId = C.Id\n'
                else:
                    fail()
            statement += 'GROUP BY C.Region\n'
            statement += 'HAVING COUNT(*) > 4\n'
            statement += 'ORDER BY '+var_s
            if (params.__contains__("bottom")):
                statement += 'ASC\nLIMIT ' + str(params["bottom"])
            elif (params.__contains__("top")):
                statement += 'DESC\nLIMIT ' + str(params["top"])
            else:
                statement += 'DESC\nLIMIT 10'
            cur.execute(statement)
            result = cur.fetchall()
            conn.close()
            return result
        except:
            return []
    else:
        return []

def print_certain_length(stri):
    result = ""
    if (isinstance(stri,str)):
        if (stri[-1] == "%"):
            result += stri + "  "
            return result
        elif (len(stri)<16):
            result += stri + " "*(15-len(stri))
        else:
            for c in stri[0:11]:
                result += c
            result += "... "
        return result
    elif (isinstance(stri,float)):
        result += str(round(stri,1)) + "  "
        return result
    elif (isinstance(stri,int)):
        result += str(stri)
        return result
    else:
        return "Unknown" + " "*8

def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        results = process_command(response)
        if (results):
            for row in results:
                out = ""
                for item in row:
                    out += print_certain_length(item)
                print (out)
                # print (row)
        elif (not response):
            print()
        elif (response == 'exit'):
            print("bye")
        else:
            print ("Command not recognized: " + response)
        if response == 'help':
            print(help_text)
            continue    

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
