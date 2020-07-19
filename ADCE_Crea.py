class ADCECrea:
    numadce = 0
    hoMarginLev = 3
    hoMarginQual = 0

    def __init__(self, BTSS, BSCidS, BCFidS, BTSidS, BTST, BSCidT, BCFidT,BTSidT,LACT,cellIdT,pbgt,umbrella):
        self.BTSS = BTSS
        self.BSCidS = BSCidS
        self.BCFidS = BCFidS
        self.BTSidS = BTSidS
        self.BTST = BTST
        self.BSCidT = BSCidT
        self.BCFidT = BCFidT
        self.BTSidT = BTSidT
        self.LACT = LACT
        self.cellIdT = cellIdT
        self.pbgt = pbgt
        self.umbrella = umbrella
        ADCECrea.numadce += 1

    def keycell (self):
        return '{}{}'.format(self.BSCidS,self.BTSidS)


class ADCEDep:
    def __init__(self, BSCidS,BCFidS,BTSidS,LACT,cellIdT):
        self.BSCidS = BSCidS
        self.BCFidS = BCFidS
        self.BTSidS = BTSidS
        self.LACT = LACT
        self.cellIdT = cellIdT
#        super().__init__(BSCidS,BCFidS,BTSidS,LACT,cellIdT)   # take info from ADCE_Crea