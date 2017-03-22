# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.

The try catch in the write isn't working, this is to prevent data loss and new
file creation if the write file locked i.e. because it is open elsewhere 
"""

import socket, time, datetime, crcmod, struct
import threading, math, json, sys, os, csv

import logging

#LOGGING
#1 debug - detailed
#2 info - confirmation
#3 warning - something unexpected
#4 error - function failed
#5 critical - function failed application close


class OperateOnDictionary(threading.Thread):
    
    def __init__(self):
       
        self.AsciiOP = False      # change to False for unicode output
        
        InvertSymmetricalComponents = True 
        self.SymAng = math.pi * (2. / 3.) 
        if InvertSymmetricalComponents == True:
            self.SymAng = -self.SymAng
        
        logging.basicConfig(filename = 'PMU2CSV_logfile.log', level = logging.INFO, filemode='w', format='%(asctime)s %(message)s')
        logging.critical('-------------------------------------')
        logging.critical('Script started at ' + time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime()) + 'GMT')
        
        threading.Thread.__init__(self)

        self.PMUip = "192.168.0.10"
        self.PMUport = 4712
        self.PMUnumber = 20
        
        self.CSVlabel = "PMUtoCSV_scriptDefault_"
        self.WriteEvery = 5
        self.CloseFileAfter = 3600

        self.timeSF = 13
        self.dataSF = 7

        
    def PullConfigData(self):
        
        def TryInsert(Dict, Key, Except, Type):
            try:
                IP = Dict[Key]
                IP = IP.replace(' ', '')
                if Type == 'float':
                    Variable = float(IP)
                    logging.info('Changing ' + str(Key) + ' to float ' + str(Dict[Key]))
                elif Type == 'int':
                    Variable = int(IP)
                    logging.info('Changing ' + str(Key) + ' to int ' + str(Dict[Key]))
                elif Type == 'bool':
                    if IP in ['True', 'true', 't', 'T', 'y', 'Y', 'Yes', 'yes']:
                        Variable = True
                    else:
                        Variable = False
                    logging.info('Changing ' + str(Key) + ' to boolean ' + str(Dict[Key]))
                else:
                    Variable = str(IP)
                    logging.info('Changing ' + str(Key) + ' to string ' + str(Variable))
                
            except:
                Variable = Except
                logging.critical('Failure to load ' + str(Variable) + ' with key ' + str(Key) + ' error ' + str(sys.exc_info()[0:2]) + ' from Dictionary ' + str(Dict.keys()))

            return(Variable)
        
        path = str(os.path.abspath(os.pardir))
        configpath = path + '/Config/PMU2CSV_config.txt'
        self.OPpath = path + '/OutputData/'
        
        DataDict = {}
        with open(configpath, 'r') as csvfile:
            data = csv.reader(csvfile, delimiter = ',')
            for row in data:
                DataDict[row[0]] = row[1]
                logging.info(str(row[0]) + '  ' + str(row[1]))
            
        self.PMUip = TryInsert(DataDict, 'PMU IP', "143.117.218.65", 'str')
        self.PMUnumber = TryInsert(DataDict, 'PMU Number', 12345, 'int')
        self.PMUport = TryInsert(DataDict, 'PMU Port', 4712, 'int')
        
        self.CSVlabel = TryInsert(DataDict, 'CSV Name', "FailName_", 'str')

        self.WriteEvery = TryInsert(DataDict, 'Write Every X Seconds', 86400, 'int')
        self.CloseFileAfter = TryInsert(DataDict, 'New File Every X Seconds', 50, 'int')

        self.timeSF = TryInsert(DataDict, 'Time Significant Figures', 13, 'int')
        self.dataSF = TryInsert(DataDict, 'Values Significant Figures', 7, 'int')
            
        try:
            if os.path.isdir(DataDict['CSV Directory']) == True:
                self.OPpath = DataDict['CSV Directory']
            elif os.path.isdir(DataDict['CSV Directory'] + '/') == True:
                self.OPpath = DataDict['CSV Directory'] + '/'
        except:
            logging.critical('Failure to load Output Directory, default to ' + str(self.OPpath))


    def SetValues(self) :     
      
        try:
            if type(self.PMUip) != str:
                self.PMUip = str(self.PMUip)
        except:
            self.PMUip = "192.168.0.20"
            
        try:
            if type(self.PMUport) != int:
                self.PMUport = int(self.PMUport)
        except:
            
            self.PMUport = 4712

        try:
            if type(self.PMUnumber) != int:
                self.PMUnumber = int(self.PMUnumber)
        except:
            self.PMUnumber = 20
            
        if os.path.isdir(self.OPpath) == False:
            os.makedirs(self.OPpath)
            


        
        self.OPlogName = 'LogFiles\\' + str(time.strftime("%Y_%m_%d_%H_%M_%S", time.gmtime()) + ' LogFile_OpenInNopepad++.txt')


        
            
    def String2list(self, StringList):
        StringList = StringList[1:-1]
        List = []
        place = StringList.find(',')
        if place < 0:
            List.append(StringList)
        else:
            while place > 0:
                place = StringList.find(',')
                if place > 0:
                    List.append(StringList[:place])
                    StringList = StringList[(place + 2):]
                else:
                    List.append(StringList)
                
        return(List)

    def Update_LocalDictionary(self, Dictionary, Value, KeyList):
        Value = str(Value)
        for n in range(0, len(KeyList)):
            KeyList[n] = str(KeyList[n])
        
        AltDict = Dictionary
        for key in KeyList[:-1]:
            if key not in AltDict:
                AltDict[key] = {}
            elif type(AltDict[key]) == dict:
                pass
            
           
            AltDict = AltDict[key]

        AltDict[KeyList[-1]] = Value
        
    def C37118_AddToCF2dict_Human(self, IDCODEsource, TIME, TIME_BASE, STN, PMUIDCODE, FORMAT, PhasorNameList, AnalogueNameList, DigitalNameList, PhasorFactorList,  AnalogueFactorList, DigitalFactorList, FNOM, CFGCNT, DATA_RATE, PMUID_LIST):
        #self.CretinsLog('C37118_AddToCF2dict_Human')        
#        MetaHelp = """SOURCE - Source of PMU stream, if direct from PMU then SOURCE = IDCODE, if PDC SOURCE != IDCODE.
#        SOC - Second of Century - time when config file was received, info only, format YYYY-MM-DD HH-MM-SS.
#        TIME_BASE - Resolution of fraction of second - divide integer in dataframe by this interger to return real value
#        STN - Station Name.
#        FNOM - Nominal Frequency, returns '60' (0 in C37 dataframe) or 50 (1 in C37 dataframe)
#        FREQ/DFREQ_NUM_TYPE - returns 'float' if frequence in dataframe is a 4 byte floating point, or 'int' if it is a 16 bit integer 
#        CTGCNT - Configuration change count
#        DATA_RATE - If > 0, then frames per second (usual), if < 0 seconds per frame (unusual)
#        
#        PHASOR_NAMES - List of the names of the contained phasors
#        PHASOR_STYLE - returns 'R' if rectangular, or 'P' if phasors are in polar format
#        PHASORS_NUM_TYPE - 'float' implies Float; 'int' implies integer, then requires Factor to scale to real 
#        PHASOR_FACTORS - if phasor number type is int, then dividing by this number gives real value, else = 0 for float type
#        
#        ANALOGUE_NAMES - List of the names of the contained analogue channels
#        ANALOGUE_NUM_TYPE - 'float' implies Float; 'int' implies integer, then requires Factor to scale to real 
#        ANALOGUE_FACTORS - if analogue number type is int, then dividing by this number gives real value, else = 0 for float type
#        
#        DIDGITAL_NAMES - List of the names of the contained digital channels (come in sets of 16)
#        DIDGITAL_FACTORS - This contains bitmaped info on digital chanels, not implemented yet!!!
#        """
#        PhasorHelp = """~'_0' etc not in data, just for graph, '_x' is only there to differentiate nodes,
#        VoI - Voltage or Current - Returns 'V' voltage or 'I' current
#        RoP - Rectangular or Polar - Returns 'R' rectangular or 'P' polar,
#        IntScale - If magnitudes are integers (not floats) then true value is returned by dividing by this number...
#        ...if value is a float then integer 0 is returned
#        """
#        AnalogueHelp = """~'_0' etc not in data, just for graph, '_x' is only there to differentiate nodes
#        MeasurementType returns 'POW' - single point on wave, 'RMS'-root mean squared, or 'PEK' - peak of analogue input
#        IntScale - If magnitudes are integers (not floats) then true value is returned by dividing by this number...
#        ...if value is a float then integer 0 is returned"""
#        DigitalHelp = """~Mask work not interpreted """   

        #self.DBname = 'V4_PMUsource' + str(IDCODEsource) + '_PMUid_' + str(PMUIDCODE) + '_' + str(len(PhasorNameList)) + 'Phasors_' + str(len(AnalogueNameList)) + 'Analogues_FrameRate_' + str(DATA_RATE)  #this is built through the config frame        
        
        def CleanNamesInList(List):
            logging.debug("list of names converted from -> " +  str(List))
            for n in range(0, len(List)):
                List[n] = str(List[n].strip())[2:-1]
            logging.debug('to -> ' + str(List))
            return(List)
            

        PhasorNameList, AnalogueNameList, DigitalNameList = CleanNamesInList(PhasorNameList), CleanNamesInList(AnalogueNameList), CleanNamesInList(DigitalNameList)

        FORMAT = bin(FORMAT)[2:].zfill(4)[-4:]     
        FNOM = int(str(bin(FNOM)[2:].zfill(32)[-32:])[-1], 2)
        TIME = datetime.datetime.fromtimestamp(int(str(TIME))).strftime('%Y-%m-%d %H:%M:%S')
        TIME_BASE = int((str(bin(TIME_BASE))[2:].zfill(24)[-24:]), 2) #strips out flags
        
        
        if FNOM == 1:
            Freq = 50
            logging.info('Base frequency = 50 Hz')
        else:
            Freq = 60
            logging.info('Base frequency = 60 Hz')
        FNOM = Freq
        
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, PMUID_LIST, ['ConfigFrame2', 'Human', IDCODEsource, 'PMUID_ORDER'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, TIME_BASE, ['ConfigFrame2', 'Human', IDCODEsource, 'TIME_BASE'])
        
        #self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, MetaHelp, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, 'Metadata', 'Help'])        
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, IDCODEsource, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'SOURCE'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, TIME, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'SOC'])        
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, STN, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, 'Metadata', 'STN'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, PhasorNameList, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN,'Metadata', 'PHASOR_NAMES'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, AnalogueNameList, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'ANALOGUE_NAMES'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, DigitalNameList, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'DIGITAL_NAMES'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, PhasorFactorList, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'ORIGINAL_PHASOR_FACTORS'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, AnalogueFactorList, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'ANALOGUE_FACTORS'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, DigitalFactorList, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'DIGITAL_FACTORS'])        
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, FNOM, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'FNOM'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, CFGCNT, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'CTGCNT'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, DATA_RATE, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'DATA_RATE'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, int(len(PhasorNameList)), ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'PHASOR_NUM_CHAN'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, int(len(AnalogueNameList)), ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'ANALOGUE_NUM_CHAN'])
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, int((len(DigitalNameList)/16)), ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'DIGITAL_NUM_CHAN'])
        
        
        if int(FORMAT[0]) == 0:
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, 'int', ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'FREQ/DFREQ_NUM_TYPE'])
        else:
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, 'float', ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'FREQ/DFREQ_NUM_TYPE'])
            
        if int(FORMAT[1]) == 0:
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, 'int', ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'ANALOGUE_NUM_TYPE'])
            AnIntFloat = 'int'
        else:
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, 'float', ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'ANALOGUE_NUM_TYPE'])
            AnIntFloat = 'float'
            
        if int(FORMAT[2]) == 0:
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, 'int', ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'PHASOR_NUM_TYPE'])
            PhIntFloat = 'int'
        else:
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, 'float', ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'PHASOR_NUM_TYPE'])
            PhIntFloat = 'float'
        if int(FORMAT[3]) == 0:
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, 'R', ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'PHASOR_STYLE'])
            PhRorP = 'R'

        else:
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, 'P', ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'PHASOR_STYLE'])
            PhRorP = 'P'

            
        n = 0

        for n in range(0, len(PhasorNameList)):

            PHUNIT = str(bin(PhasorFactorList[n])[2:].zfill(32)[-32:])
            PhScale = 0
            
            if int(PHUNIT[0:8],2) == 0:
                PhVoI = 'V'
                if PhIntFloat == 'int':
                    PhScale = int(PHUNIT[8:],2)
                    PhScale = float(PhScale) / 100000.0
            else:
                PhVoI = 'I'
                if PhIntFloat == 'int':
                    PhScale = int(PHUNIT[8:],2)
                    PhScale =  float(PhScale) / 100000.0 
                    Scale = 1.
                    AmpPerBit = 1.

            PhasorFactorList[n] = PhScale
            
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, PhVoI, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Phasors', PhasorNameList[n],'VorI'])
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, PhScale, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Phasors', PhasorNameList[n],'IntScale'])
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, PhRorP, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Phasors', PhasorNameList[n],'RorP'])
            #self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, (str(PhasorNameList[n]) + PhasorHelp), ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN,'Phasors',  PhasorNameList[n],'Help'])
            if PhVoI == 'V':
                logging.info(str(PhasorNameList[n]) + ' Monitoring ' + str(PhVoI) + ' | Scale factor = ' + str(PhScale) + ' | format ' + str(PhRorP))
            else:
                logging.info(str(PhasorNameList[n]) + ' Monitoring ' + str(PhVoI) + ' | Scale factor = ' + str(Scale) + ' | Amp Per Bit = ' + str(AmpPerBit) + ' | format ' + str(PhRorP))
        
        self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, PhasorFactorList, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Metadata', 'PHASOR_FACTORS'])
        logging.debug(str(PhasorNameList))
        logging.debug(str(PhasorFactorList))
        
        for n in range(0, len(AnalogueNameList)):
            MType = int(str(bin(AnalogueFactorList[n])[2:].zfill(32)[-32:])[0:8],2)
            binaryScale = str(bin(AnalogueFactorList[n])[2:].zfill(32)[-32:])[8:]
            Scale = float(int(binaryScale ,2))
            
            MeasureType = 'POW'
            if MType == 1:
                MeasureType = 'RMS'
            if MType == 2:
                MeasureType = 'PEK'
               
            AnScale = 0
            if AnIntFloat == 'int':
                AnScale = 100000.0/Scale
            
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, MeasureType, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Analogues', AnalogueNameList[n],'MeasurementType'])
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, AnScale, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Analogues', AnalogueNameList[n],'IntScale']) 
            #self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, (str(AnalogueNameList[n]) + AnalogueHelp), ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Analogues', AnalogueNameList[n],'Help']) 
            logging.info('Analogue ' + str(AnalogueNameList[n]) + ' Measuring ' + str(MeasureType) + ' with scale ' + str(AnScale))
        
        for n in range(0, len(DigitalNameList)):
            """!!!!!!!!!!!!!!!!!!!!!This might not be working!!!!!!!!!!!!!!!!!!!"""
            self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, DigitalFactorList, ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Digitals', DigitalNameList[n], 'MaskWord'])
            #self.Update_LocalDictionary(self.PMUconfig_LocalDictionary, (str(DigitalNameList[n]) + DigitalHelp), ['ConfigFrame2', 'Human', IDCODEsource, PMUIDCODE, STN, 'Digitals', DigitalNameList[n],'Help']) 
            #logging.info('Digital name ' + str(DigitalNameList[n] + )
        logging.debug(str(self.PMUconfig_LocalDictionary))

    def C37118_AddToDFdict_Human(self, SYNC, FRAMESIZE, PDCID, PMUID, SOC, FRACSEC, CHK, Measurements):
        """Digitals have not been unpacked"""
        
        def Rec2Polar(X, Y):
            """worth double checking this, it will be internall consistent, 
            make sure it is globally consistent"""
            Mag = (X**2 + Y**2)**0.5
            Ang = math.atan2(Y, X)   
            
            return(Mag, Ang)
            
        FRACSEC = str(bin(int(FRACSEC)))[2:].zfill(32)[-32:]
        TimeQuality = FRACSEC[:8]
        FRACSEC = int(FRACSEC[8:],2)
        
        
        FracSec = float(FRACSEC) / int(self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)]['TIME_BASE'])

        Time = SOC + FracSec
        
        if Time > self.CurrentTime:
            self.CurrentTime = Time 

        if Time not in self.Temp_PMU_DF_dict:
            self.Temp_PMU_DF_dict[Time] = {}

        if self.Time1 != Time:
            if self.Time2 != None:
                self.Time2 = self.Time1
                self.Time1 = Time
                deltaT = (int(0.5 + 1000 * (self.Time1 - self.Time2)))
                # self.InfoList.append(deltaT)

                if deltaT > 1000 and self.InitialiseTime == True:
                    self.InitialiseTime = False
                    deltaT = 0

            else:
                self.Time1 = Time
                self.Time2 = self.Time1

        n = -1
        Measurements = list(Measurements)
        
        

        if self.PMUlist != ['']:
            
            for PMUIDCODE in self.PMUlist:
                STN = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)]['Metadata']['STN']

                if int(TimeQuality,2) > 0:
                    self.Temp_PMU_DF_dict[Time][(PDCID, PMUID, STN, 'Digitals', 'TimeQuality')] = TimeQuality
                
                STAT = Measurements.pop(0)
                self.Temp_PMU_DF_dict[Time][(PDCID, PMUID, STN, 'Digitals', 'STAT')] = STAT
                
                PhNameLst = self.String2list(self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Metadata']['PHASOR_NAMES'])
                PhFactLst = self.String2list(self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Metadata']['PHASOR_FACTORS'])
                PhNumType = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Metadata']['PHASOR_NUM_TYPE']

                if PhNameLst != ['']:
                    VmagList, VangList = [], []
                    for n in range(0, len(PhNameLst)):
                        PhasorName = PhNameLst[n]
                        PhasorFactor = float(PhFactLst[n])
                        
                        PhasorName = str(PhasorName)[1:-1]
                        RorP = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Phasors'][PhasorName]['RorP']
                        VorI = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Phasors'][PhasorName]['VorI']
                                                
                        X = float(Measurements.pop(0))
                        Y = float(Measurements.pop(0))
                                                    
                        Mag, Ang = X, Y
                        """This is correct if the DF is float and in Polar,
                        else it will be written over - Polar is prefered"""
                        if PhNumType == 'int':
                            if RorP == 'R':
                                RawX, RawY = float(X), float(Y)
                                X, Y = RawX * PhasorFactor, RawY * PhasorFactor
                                Mag, Ang = Rec2Polar(X, Y)
                            else:
                                Mag, Ang = float(X) * PhasorFactor, float(Y)/10000.0                            
                        else:
                            if RorP == 'R':
                                Mag, Ang = Rec2Polar(X, Y)
    
                        self.Temp_PMU_DF_dict[Time][(PDCID, PMUID, STN, 'Phasors', VorI, PhasorName, 'Mag')] = Mag
                        self.Temp_PMU_DF_dict[Time][(PDCID, PMUID, STN, 'Phasors', VorI, PhasorName, 'Ang')] = Ang
                        
                        if VorI == 'V':
                            VmagList.append(Mag)
                            VangList.append(Ang)
                    
                FREQ = Measurements.pop(0)
                DFREQ = Measurements.pop(0)
                
                FqNumSty = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Metadata']['FREQ/DFREQ_NUM_TYPE']
                if FqNumSty == 'int':
                    try:
                        FREQ, DFREQ = (self.FNOM + float(FREQ)/1000), float(DFREQ)/100
                    except:
                        self.FNOM = int(self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Metadata']['FNOM'])
                        FREQ, DFREQ = (self.FNOM + float(FREQ)/1000), float(DFREQ)/100
                                                
                self.Temp_PMU_DF_dict[Time][(PDCID, PMUID, STN, 'Analogues', 'Freq')] = FREQ
                self.Temp_PMU_DF_dict[Time][(PDCID, PMUID, STN, 'Analogues', 'dFdt')] = DFREQ
                                   
            AnList = self.String2list(self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Metadata']['ANALOGUE_NAMES'])  

            if AnList != ['']:
                for AnalogueName in AnList:
                    AnalogueName = str(AnalogueName)[1:-1]

                    IntScaleAn = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Analogues'][AnalogueName]['IntScale']
                    TypeAn = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Analogues'][AnalogueName]['MeasurementType']
                    M = Measurements.pop(0)
                    
                    
                    if IntScaleAn != 0:
                        """if IntScaleAn equals zero the value is transmitted
                        as a float and should already be interpreted as such,
                        otherwise the value is an interger that need to be moderated down"""
                        X = float(M)/float(IntScaleAn)

                    self.Temp_PMU_DF_dict[Time][(PDCID, PMUID, STN, 'Analogues', 'Analogue Chs', AnalogueName, TypeAn)] = X

            DgList = self.String2list(self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(PMUIDCODE)][STN]['Metadata']['DIGITAL_NAMES'])  
            if DgList != ['']:
                #Digitals not parsed at present!!!
                DgList = [str(DgList)]
                for DgChName in DgList:
                    DIGITAL = Measurements.pop(0)
                    self.Temp_PMU_DF_dict[Time][(PDCID, PMUID, STN, 'Digitals', DgChName)] = DIGITAL
        


class Connect2PMU(OperateOnDictionary):
    """This is to open the communication path to the PMU"""           
    def __init__(self):
        OperateOnDictionary.__init__(self)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(1.0)
        
    def OpenTCPcomms(self):
        self.serversocket = self.sock
        Connected = False
        Attempts = 0
        while Connected == False and Attempts <15:
            #self.serversocket.connect((self.PMUip, self.PMUport))
            try:
                #print(self.PMUip, self.PMUport)
                self.serversocket.connect((self.PMUip, self.PMUport))
                Connected = True
                logging.critical('Connected to PMU')
                
            except:
                e = sys.exc_info()[0:2]
                logging.critical('TCP connection failed with ' + str(e) + ' Attempt ' + str(Attempts)) 
                time.sleep(0.25)
                Attempts += 1
                self.CloseTCPcomms()
        
    def CloseTCPcomms(self):
        self.serversocket.close()        
        logging.warning('TCP comms closed') 
        
class CommandFrames(Connect2PMU):
    
    def __init__(self):
        Connect2PMU.__init__(self)
        self.TCPIPsendList = []
        self.crcFunc = crcmod.mkCrcFun(0x11021, rev=False, initCrc=0xFFFF, xorOut=False)        
        
    def CommFrame(self, command):
        """Command frame 2 requests active values only
        for ip 192.168.0.20 and port 4712 with device ID 20
        the command frame, in Hex, should be
        AA 41 00 12 00 14 56 2E 8E C4 00 01 72 8F 00 05 BE 07"""
        
        
        
        SYNC = 43585         #0xAA41 in Hex   
        FRAMESIZE = 18       #bytes - 0012 in Hex
        IDCODE = self.PMUnumber   #PMU number 
        SOC = int(time.time())      #UNIX time secons since 1970-01-01
        FRACSEC = 0      #I don't think this is too important, 2 bytes frac sec, 2 bytes time sync error
        CMD = command
        CHK = (self.crcFunc(struct.pack("!3H2LH", SYNC, FRAMESIZE, IDCODE, SOC, FRACSEC, CMD)))
        PACKET = struct.pack("!3H2L2H", SYNC, FRAMESIZE, IDCODE, SOC, FRACSEC, CMD, CHK)  
        
        return(PACKET)
        
       
     
    def StopPMUstream(self):
        logging.info('Sending Stop PMU Stream Command Frame') 
        return(self.CommFrame(1))
    def StartPMUstream(self):
        logging.info('Sending Start PMU Stream Command Frame') 
        return(self.CommFrame(2))
    def SendHDR(self):
        logging.info('Request Header Command Frame') 
        self.CommFrame(3)    
    def SendCFG1(self):
        logging.info('Request Config Frame 1 - All possible channels') 
        self.CommFrame(4) 
    def SendCFG2(self):
        return(self.CommFrame(5))
    def SendCFG3(self):
        self.CommFrame(6)

       
        
class DecodeC37(CommandFrames):
    
    def __init__(self):
        CommandFrames.__init__(self)
        self.DataPacket = False
        self.Update = False
        self.JointFrame = []
       
    def C37dataEnter(self, Packet):

        StructCode = '!' + str(int(len(Packet) / 2)) + 'H'
        PacketShortInt = struct.unpack(StructCode, Packet)
        ID = bin(PacketShortInt[0])[2:].zfill(16)[9:-4]
        FRAMESIZE = PacketShortInt[1]
        IDCODEsource = PacketShortInt[2]
        CHK = self.crcFunc(Packet)
        
        if CHK == 0:
            proceed = True
            if len(self.JointFrame) > 0:
                """If previous packets have not been processed, but this one
                is good, then the bad packets are dropped"""
                logging.warning('Good Package found - dropping ' + str(len(self.JointFrame)) + ' Packets')
                self.JointFrame = []
            
        elif CHK != 0: 
            """If the check sum is bad then it is assumed apacket has been 
            chopped, this is an attempt to recombine - this may take more
            inginuity, but might not be needed"""
            logging.warning('C37dataEnter Packet Error, check sum ' + str(CHK)) 
            logging.warning("Expected Length -" + str(FRAMESIZE) + "Actual Length -" + str(len(Packet))) 
            proceed = False
            try:
                for n in range(0, len(self.JointFrame)):
                    self.JointFrame[n] = self.JointFrame[n] + Packet
                self.JointFrame.append(Packet)                
            except:
                self.JointFrame = [Packet]
                
            
            for n in range(0, len(self.JointFrame)):
                if self.crcFunc(self.JointFrame[n]) == 0:
                    """The packet has successfully been rebuilt"""
                    proceed = True
                    logging.warning('Packet rebuilt from ' + str(n) + ' in list') 
                    Packet = self.JointFrame[n]
                    if n == 0:
                        logging.warning('Package salvaged, no loss')
                    else:
                        logging.warning('Package salvaged ' + str(n) + ' Packets dropped')
            
        if proceed == True:
            if ID == '000':
                #logging.debug('C37dataEnter Data Frame Packet')            
                try:
                    self.C37dataFrameEnter(Packet, FRAMESIZE, IDCODEsource)
                except:
                    e = sys.exc_info()[0:2]
                    logging.critical('Data Frame entry complete fail, requesting new CF2, failed with')
                    logging.critical(str(e))
                    self.TCPIP = ""
                    self.serversocket.send(self.SendCFG2())
                    self.serversocket.send(self.StartPMUstream())
                
                if self.DataPacket == False:
                    self.DFsize = FRAMESIZE
                    logging.warning('C37dataEnter Data Frame Packet received and processing')
                    self.DataPacket = True
                    
            elif ID == '011':
                """Config Frame 2 - this is the one that tells you what will be 
                transmitted, not what can be transmitted by the device"""
                self.DataPacket = False
                self.StructCode = ''
                
                logging.info('Received C37dataEnter Command Frame 2 Packet')
                self.ConfigFrame2Length = len(Packet)
                
                NUM_PMU = PacketShortInt[9]
                PHNMR, ANNMR, DGNMR = PacketShortInt[20], PacketShortInt[21], PacketShortInt[22]
                logging.critical('Entering CF2 with ' + str(NUM_PMU) + ' PMUs ' + str(PHNMR) + ' Phasors ' + str(ANNMR) + ' Analogues ' + str(DGNMR) + ' Digitals')
                self.C37configFrame2Enter(Packet, NUM_PMU, PHNMR, ANNMR, DGNMR)
                #with open('ConfigFrame2.txt', 'w') as outfile:
                 #   json.dumps(self.PMUconfig_LocalDictionary, outfile)
                    
                
            elif ID == '001':
                self.DataPacket = False
            elif ID == '010':
                self.DataPacket = False
            elif ID == '100':
                self.DataPacket = False
            elif ID == '101':
                self.DataPacket = False
            else:
                logging.warning('C37dataEnter Unrecognised Packet')
                self.DataPacket = False

        
    def PrintDictionary(self, Dict):
        PrtList = []
        
        for Key in Dict:
            PrtList.append([str(Key), str(Dict[Key])])
        PrtList = sorted(PrtList)
        for element in PrtList:
            print(str(element))
        
    def C37dataFrameEnter(self, Packet, FRAMESIZE, IDCODEsource):
        try:
            self.FirstDF = False
            C37DF = struct.unpack(self.StructCode, Packet)  

            SYNC, FRAMESIZE, IDCODE, SOC, FRACSEC, CHK = C37DF[0], C37DF[1], C37DF[2], C37DF[3], C37DF[4], C37DF[-1]            
            Measurements = C37DF[5:-1]
        
            self.C37118_AddToDFdict_Human(SYNC, FRAMESIZE, IDCODEsource, IDCODE, SOC, FRACSEC, CHK, Measurements)
            
        except:
            e = sys.exc_info()[0:2]
            logging.warning('Data Frame entry error - normal after receit of CF2')
            logging.warning(str(e))
            PMUlist = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(IDCODEsource)]['PMUID_ORDER']
            self.PMUlist = self.String2list(PMUlist)
            Fields6to11 = ''
            
            
            for PMUID in self.PMUlist:
                STN = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(PMUID)][str(IDCODEsource)]['Metadata']['STN']
                FqNumSty = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(IDCODEsource)][str(PMUID)][STN]['Metadata']['FREQ/DFREQ_NUM_TYPE']
                
                PhNumSty = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(IDCODEsource)][str(PMUID)][STN]['Metadata']['PHASOR_NUM_TYPE']
                RorP = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(IDCODEsource)][str(PMUID)][STN]['Metadata']['PHASOR_STYLE']
                PhNumChn = int(self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(IDCODEsource)][str(PMUID)][STN]['Metadata']['PHASOR_NUM_CHAN'])
                
                AnNumSty = self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(IDCODEsource)][str(PMUID)][STN]['Metadata']['ANALOGUE_NUM_TYPE']
                AnNumChn = int(self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(IDCODEsource)][str(PMUID)][STN]['Metadata']['ANALOGUE_NUM_CHAN'])
                
                DiNumChn = int(self.PMUconfig_LocalDictionary['ConfigFrame2']['Human'][str(IDCODEsource)][str(PMUID)][STN]['Metadata']['DIGITAL_NUM_CHAN'])
                
                
                if FqNumSty == 'int':
                    FqStruct = 'h'
                    logging.info('Frequency in interger form')
                else:
                    FqStruct = 'f'
                    logging.info('Frequency in float form')
                    
                
                if PhNumSty == 'int':
                    if RorP == 'R':
                        PhStruct = PhNumChn * 'hh'
                        logging.info('Phasor in Rectangular Interger form')
                    else:
                        PhStruct = PhNumChn * 'Hh'
                        logging.info('Phasor in Polar Interger form')
                else:
                    PhStruct = PhNumChn * 'ff'
                    logging.info('Phasor in Float form')
                    
                
                if AnNumSty == 'int':
                    AnStruct = AnNumChn * 'h'
                    logging.info('Analogues in Interger form')
                else:
                    AnStruct = AnNumChn * 'f'
                    logging.info('Analogues in Float form')
                    
                DiStruct = DiNumChn * 'H'
                Fields6to11 += 'H' + PhStruct + FqStruct + FqStruct + AnStruct + DiStruct
                
            self.StructCode = '!3H2L' + Fields6to11 + 'H'
            logging.warning('Struct code created - ' + str(self.StructCode))

            C37DF = struct.unpack(self.StructCode, Packet)  

            logging.debug('Packet unpacked ' + str(C37DF))
    
            SYNC, FRAMESIZE, IDCODE, SOC, FRACSEC, CHK = C37DF[0], C37DF[1], C37DF[2], C37DF[3], C37DF[4], C37DF[-1]            
            Measurements = C37DF[5:-1]
            
            logging.debug('Sending packet to local data dictionary')
            self.FirstDF = True
            self.C37118_AddToDFdict_Human(SYNC, FRAMESIZE, IDCODEsource, IDCODE, SOC, FRACSEC, CHK, Measurements)
            
    def C37configFrame2Enter(self, Packet, NUM_PMU, PHNMR, ANNMR, DGNMR):
       
        Fields8to19 = '16s5H' + '16s' * (PHNMR + ANNMR + 16 * DGNMR)  \
        + 'L' * PHNMR + 'L' * ANNMR + 'L' * DGNMR + '2H' 
        
        StructCode = '!3H3LH' + NUM_PMU * Fields8to19 + '2H'
        
        logging.info('CF" Struct Code = ' + str(StructCode))
        
        C37CF2 = struct.unpack(StructCode, Packet)
        
        logging.info('Unpacked CF2 Packet = ' + str(C37CF2))
        IDCODEsource = C37CF2[2]
        TIME = C37CF2[3]
        TIME_BASE = C37CF2[5]        
        Measurements = list(C37CF2[7:-2])
        DATA_RATE = C37CF2[-2]
        
        stop = False
        
        PMUID_LIST = []
        while stop == False:
            STN = str(Measurements.pop(0).strip())[2:-1]
            PMUIDCODE = Measurements.pop(0)
            PMUID_LIST.append(PMUIDCODE)
            FORMAT = Measurements.pop(0)
            PHNMR = Measurements.pop(0)
            ANNMR = Measurements.pop(0)
            DGNMR = Measurements.pop(0)
            
            PhasorNameList = Measurements[0:PHNMR]
            Measurements = Measurements[PHNMR:]
            AnalogueNameList = Measurements[0:ANNMR]
            Measurements = Measurements[ANNMR:]
            DigitalNameList = Measurements[0:(16*DGNMR)]
            Measurements = Measurements[(16*DGNMR):]
            
            PhasorFactorList = Measurements[0:PHNMR]
            Measurements = Measurements[PHNMR:]
            AnalogueFactorList = Measurements[0:ANNMR]
            Measurements = Measurements[ANNMR:]
            DigitalFactorList = Measurements[0:DGNMR]
            Measurements = Measurements[DGNMR:]
            
            FNOM = Measurements.pop(0)
            CFGCNT = Measurements.pop(0)
            
            self.C37118_AddToCF2dict_Human(IDCODEsource, TIME, TIME_BASE, STN, PMUIDCODE, FORMAT, PhasorNameList, AnalogueNameList, DigitalNameList, PhasorFactorList,  AnalogueFactorList, DigitalFactorList, FNOM, CFGCNT, DATA_RATE, PMUID_LIST)
            self.ReInitialiseDF = True
            if len(Measurements) == 0:
                stop = True
                
                        
class Threading_Functions(DecodeC37):
    
    def __init__(self):
        DecodeC37.__init__(self)
        self.TCP_IP_List = []
        self.TCPIP = b""
        self.Topic2 = []

    def TCP_Socket(self):
        
#        TCPIP = self.serversocket.recv(1024)
#        while True:
#            self.TCPIP += TCPIP
#            if len(TCPIP) == 1024:
#                TCPIP = self.serversocket.recv(1024)
#            else:
#                break

        if self.Exit == False:
            try:
                TCPIP = self.serversocket.recv(4096)
                self.TCPIP += TCPIP
            except:
                pass       
      
    def TCP_DF_Listener(self):

        """This carries out the normal TCP entering and parsing, further error
        handling here could be advantagious"""
        
        if len(self.TCPIP) >= 20:
            #print(len(self.TCPIP))
            self.Update = True
            """This first bit is just to test buffers"""
            self.DFinBuffer = len(self.TCPIP)
            
            ByteElement1 = self.TCPIP[0]
            while ByteElement1 != 170:
                """This chops out any errand bytes at the start"""
                self.TCPIP = self.TCPIP[1:]
                ByteElement1 = self.TCPIP[0]
            ByteElement2 = bytes([self.TCPIP[2]]) + bytes([self.TCPIP[3]])
            FrameLength = struct.unpack('!H', ByteElement2)[0]

            while len(self.TCPIP) >= FrameLength:
                """This should snip the TCP string into packets starting with
                170 or AA."""
                
                ByteElement1 = self.TCPIP[0]
                while ByteElement1 != 170:
                    """This chops out any errand bytes at the start"""
                    self.TCPIP = self.TCPIP[1:]
                    ByteElement1 = self.TCPIP[0]
                ByteElement2 = bytes([self.TCPIP[2]]) + bytes([self.TCPIP[3]])
                
                FrameLength = struct.unpack('!H', ByteElement2)[0]

                self.TCP_IP_List.append(self.TCPIP[:FrameLength])
                self.TCPIP = self.TCPIP[FrameLength:]

                
    def TCP_to_Dict(self):
            
        while len(self.TCP_IP_List) > 0:
            """Clear the data/config frame queue, by firing frames for entry"""
            C37DF = self.TCP_IP_List.pop(0)
            self.C37dataEnter(C37DF)

    def ThreadWrite(self, Array, Time):
        def Write(Array, Name):
            Name = self.OPpath + self.CSVlabel + str(Time) + '.csv'

            with open(Name, 'a', newline = '') as f:
                writer = csv.writer(f)
                writer.writerows(Array)
                
        t = threading.Thread(target = Write, args = (Array, Time))
        t.start()

    def SigFigures(self, List):
        """I am assuming that the time is first (default 13 SF) then everything
        else is data (default 7 SF) - yey, big boy Python!!!"""
        fix2 = lambda Value, SF : str(float(int(float(Value) * 10**(SF) + 0.5))/10**(SF))
        fix = lambda Value, SF : str(int(Value + 0.5)) if (SF - str(Value).find('.')) < 0 else fix2(Value, (SF - str(Value).find('.')))
        return( [ fix(List[n], self.timeSF) if n == 0 else fix(List[n], self.dataSF) for n in range(len(List)) ] )        
        
    def ArrayMe(self, Dict, TimeList, Headers):
        WriteArray = []
        for Time in TimeList:
            TempList = [Time]
            LitlDict = Dict[Time]
            for key in Headers:
                TempList.append(LitlDict[key])
            WriteArray.append(self.SigFigures(TempList))
        return(WriteArray)
            
    def WriteData(self):
        TimeList = []
        Headers = []
        try:
            if len(self.WriteDict) > 0:
                
                for key1 in self.WriteDict:
                    TimeList.append(key1)
                if len(TimeList) > 0:
                    TimeList.sort()
                    for key2 in self.WriteDict[TimeList[0]]:
                        Headers.append(key2)
                    Headers.sort()
                    if self.NewFile == True:
                        HeadersOP = [ str(data[4:]) for data in Headers ]
                        self.NewFile = False
                        CutIndex = 0
                        Hit = False
                        for Time in TimeList:
                            if (int(Time*1000 + 0.5)/1000.) % self.CloseFileAfter == 0:
                                Hit = True
                                break
                            else:
                                CutIndex += 1
                                
                        if Hit == True :
                            TimeList1 = TimeList[:CutIndex]
                            self.ThreadWrite(self.ArrayMe(self.WriteDict, TimeList1, Headers), self.WriteTime)
                            TimeList2 = TimeList[CutIndex:]
                            self.WriteTime = int(TimeList2[0])
                            Array = [['Time UTC'] + HeadersOP] + self.ArrayMe(self.WriteDict, TimeList2, Headers)
                            self.ThreadWrite(Array, self.WriteTime)
                            logging.info('New File ' + str(self.WriteTime) + ' | Headers = ' + str(HeadersOP))  
                        else:
                            self.WriteTime = int(self.CurrentTime)
                            logging.info('Start File ' + str(self.WriteTime) + ' | Headers = ' + str(HeadersOP))                    
                            Array = [['Time UTC'] + HeadersOP] + self.ArrayMe(self.WriteDict, TimeList, Headers)
                            self.ThreadWrite(Array, self.WriteTime)
                        
                        #Add function to check size of folder and decimate if necessary
                            
                    else:
                        self.ThreadWrite(self.ArrayMe(self.WriteDict, TimeList, Headers), self.WriteTime)
                self.WriteDict = {}
                if self.WriteError == True:
                    self.WriteError = False
                    logging.critical('Back writing again')
        except:
            if self.WriteError == False:
                self.WriteError = True
                logging.critical('Error in writing to file, is it open in something other than NotePad++??? You will be notified if service resumes...')
                logging.critical(str(sys.exc_info()[0:2]))
     
class Threading_Operation(Threading_Functions):
    
    def __init__(self):
        Threading_Functions.__init__(self)
        
    def Get_CF2_and_initialise(self):
        try:
            self.CloseTCPcomms()
        except:
            pass

        self.OpenTCPcomms()
        self.serversocket.send(self.SendCFG2())

        CF2 = b''
        X = self.serversocket.recv(1024)
        while True:
            CF2 += X
            try:
                X = self.serversocket.recv(1024)
                if X == b'':
                    break
            except:
                break
            
        self.C37dataEnter(CF2)
        logging.critical('Connected, Command Frame 2 received and processed')
        
    def Go(self):

        while True:
            self.PullConfigData()
            self.Update = False
            self.CurrentTime = 0
            self.Exit = False
            Log = True
            self.WriteDict = {}
            self.InitialiseTime = True
            self.PMUconfig_LocalDictionary = {}  
            self.Temp_PMU_DF_dict = {}
            self.Time1 = -1.
            self.Time2 = None
            try:                
                logging.critical('Comms Thread Started')
                self.WriteTime = None
                self.Temp_PMU_DF_dict = {}
                self.Get_CF2_and_initialise()
                self.serversocket.send(self.StartPMUstream())
                #self.ExitDataIP = False 
                #self.LoopProcess = True
                self.WriteError = False
                
                self.NewFile = True
                
                DataAttempts = 1
                OldMod = -1
                lastTime = 0
                logging.critical('Initialised, Data Frame processing loop')
                while self.Exit == False:
                    try:
                        Log = True
                        self.TCP_Socket()
                        self.TCP_DF_Listener()
                        self.TCP_to_Dict()
                        self.WriteDict.update(self.Temp_PMU_DF_dict)
                        self.Temp_PMU_DF_dict = {}
                        
                        if (self.CurrentTime - lastTime) >= self.WriteEvery:
                            lastTime = self.CurrentTime
                            NewMod = int(self.CurrentTime) % self.CloseFileAfter
                            if NewMod < OldMod: #86400 IS ONE DAY IN SECONDS
                                """This means time has ticked over"""
                                self.NewFile = True                                
                            
                            OldMod = NewMod                                
                            logging.debug('writing')
                            self.WriteData()
                        
                        if self.Update == False:
                            time.sleep(0.005)
                        self.Update = False
                        
                    except:
                        e = sys.exc_info()[0:2]
                        logging.critical('Data reader thread failed with ' + str(e) + ' Attempt ' + str(DataAttempts)) 
                        self.WriteDict.update(self.Temp_PMU_DF_dict)                        
                        self.Temp_PMU_DF_dict = {}
                        self.WriteData()
                        time.sleep(0.1)
                        DataAttempts +=  1
                        if DataAttempts >= 10:
                            self.Exit = True
                    
    
                logging.critical('EXIT!!! - PMU connection thread')
            except:
                if Log == True:
                    Log = False
                    logging.critical('Failure in main comms thread, retrying every 15 seconds')
                self.WriteDict.update(self.Temp_PMU_DF_dict)
                self.Temp_PMU_DF_dict = {}
                self.WriteData()               
                
                time.sleep(15)
                
        logging.critical('Main Loop finished')
           
        
if __name__ == '__main__':
    run = Threading_Operation()     
    run.Go()
