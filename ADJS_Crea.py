class ADJSCrea:
    numadjs = 0
    mcc = 732
    mnc = 101
    rthop = 20
    nrthop = 21
    hshop = 22
    hsrthop =20
    def __init__(self,sourcerncid,sourceci,sourcename,targetcelldn,targetname):
        self.sourcerncid = sourcerncid
        self.sourceci = sourceci
        self.sourcename = sourcename
        self.targetcelldn = targetcelldn
        self.targetname = targetname
        ADJSCrea.numadjs += 1

    def keycell (self):
        return '{}{}'.format(self.sourcerncid,self.sourceci)

class ADJSDep (ADJSCrea):
    def __init__(self, sourcerncid,sourceci,sourcename,targetcelldn,targetname, wbtsids, adjsid):
        super().__init__(sourcerncid,sourceci,sourcename,targetcelldn,targetname)   #take info from ADJS_Crea
        self.wbtsids = wbtsids
        self.adjsid = adjsid
