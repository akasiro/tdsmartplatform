import os


def genfilelist(date):
    with open('input/l1.txt', 'r') as f:
        l1 = f.readlines()
        l1 = [i.replace('\n','') for i in l1 if i != '\n']
    with open('input/l2.txt','r') as f:
        l2 = f.readlines()
        l2 = [i.replace('\n', '') for i in l2 if i != '\n']
    with open('output/{}.txt'.format(date),'w') as f:
        f.write('l1 = {}\n\n'.format(str(l1)))
        f.write('l2 = {}\n\n'.format(str(l2)))
        f.write('date = "{}"'.format(date))

if __name__ == "__main__":
    genfilelist(20171231)
