
class Node(object):
    def __init__(self,left=None,right=None,op=None,parent=None):
        self.parent=parent
        self.val=None
        self.left=left
        self.right=right
        self.op=op
        self.leaf=None

    def print_tree(self):
        pass

    def __str__(self):
        return ("op:%s,left:%s,righ:%s" %(self.op,self.left,self.right))


def cross(data):
    """
    输入两个数字，绑定是否穿过0
    :param data:
    :return:
    """

    if (data[0] <0 and data[1] >0) or (data[0] >0 and data[1] <0) :
        return True
    else:
        return False

def up(data):
    """判断数组的数值是否连续增长"""
    for i in range(1,len(data)):
        if data[i-1]> data[i]:
            return False
    return True

def down(data):
    """判断数组的数值是否连续增长"""
    for i in range(1,len(data)):
        if data[i-1]<data[i]:
            return False
    return True

def above(x,y):
    pass

def below(x,y):
    pass
