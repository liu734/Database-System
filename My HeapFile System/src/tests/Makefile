JDKPATH = /usr
LIBPATH = ../../lib/heapAssign.jar

CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH):../heap

BINPATH = $(JDKPATH)/bin

JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java -classpath $(CLASSPATH)

PROGS = heap HFTest xx

all: $(PROGS)

heap: ../heap/*.java
	$(JAVAC) ../heap/*.java


HFTest: HFTest.java 
	$(JAVAC) TestDriver.java HFTest.java


xx : HFTest 
	$(JAVA) -cp $(CLASSPATH):bin tests.HFTest

clean:
	\rm -f *.class *~ \#* core TRACE