# PMUtoCSVwriter
This communicates with a PMU in C37.118 format and writes the data to a CSV file

Requirements, 
	* TCP/IP access to the PMU, try pinging and test network connections (start simple).

The code can be executed from command line, a console or as a daemon/service.
Being Python it is cross platform, but it is written in Python 3.
Every effort has been made to reduce the dependencies and use native libraries.

Usage

1) Enter Relevant Data in the PMU2CSV_config.txt file in the /Config directory
	* PMU IP - the IP address is an obvious requirement
	* PMU Number - (or ID) is required and user configurable at PMU generally
	* PMU Port - tends to be 4712, but in theory can change
	* CSV Name - This is the name given to the CSV file, the time in UTC seconds is appended to the name with .csv
	* CSV Directory - The default path is the directory main directory, the default write directory is Output
	* Time Significant Figures - This is the number of significant figures in the UTC time, we are now around the 1.5 billion second mark, so 1,500,000,000, so 13 significant figures goes to milliseconds for the next few centuries - on 60 Hz systems you might want to bump this up to 15 or 16, personally I'd change to a base 12 number system, but given the slow adoption on the metric system..., what you gonna do?
	* Value Significant Figures - All values are truncated to 7 significant figures or Zero decimal places, whichever is more precise - this is to save meaningless bloat in the CSV file when storinf floats as strings
	* Write Every X SEconds - this is how often the file is updated, default to 5 seconds
	* New File Every X SEconds - this is how often the file is closed, 3600 is every hour, 86400 is once a day - 1 hour with 8 phasors is (i'll get back to this)
	
	The config file is pulled as a CSV, converted to a dictionary and the values entered

	
2) Execute the PMUtoCSV_VerA01.py script
	`python3 PMUtoCSV_VerA01.py`  
	The script will attempt a connection with the PMU, there are two loops.
	
	* LOOP 1 - Initialisation loop
		* This loop will open a new connection to the PMU and clear all previous data.
		* An exception in this loop results in an emergengy data write followed by a 15 second cool of
		* Errors in this loop are reported in the log file
		* A new CSV file will be created when this loop is initiated
		
	* Loop 2 - Data Collection Loop
		* This manages the TCP/IP stream, C37.118 to information and PMU information to CSV process
		* If an error occurs in this loop, then a 100 ms cool off is applied 10 times
		* 10 unsucessfull loops causes this loop to break
		* While this loop is not broken the write file remains the same, the error is reported to the log file
	
	
3) The Log Files
	* Log files are set to overwrite ('w') not to append ('a') this can be changed
	* The logging level is set to INFO, this does not bloat the log file, only INFO and CRITICAL have been used in the code
	* To change the level of logging open the script and change "INFO" TO "CRITICAL", not hard to find
	* The log is the place to check the health of the script. 


4) Causes of Exceptions

	1) Network error - should be handled, data will unavoidably be lost, reconnection should be established in Loop 1 or 2
	2) Opening the write file in a program that locks it (e.g. Excel, but not NotePad++) - not well handled, data can be lost, this could be improved
	3) PMU configuration changes, i.e. someone changes phasor properties mid-stream, this should be caught
	4) Disk Full - not catered for at the moment, possible implementations delete oldest file based on directory size, age of file, free space
	5) ...

