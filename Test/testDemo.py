if __name__ == '__main__':
    filename = "D:/project/dbsync/apiService/config.json"
    with open(filename,'r') as fileobject:
        lines = fileobject.readlines()
        for line in lines:
            print line