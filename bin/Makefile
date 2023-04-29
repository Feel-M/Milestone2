
JFLAGS = -g
JC = javac

.SUFFIXES: .java .class

.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
		  DBApp.java \
		  exceptions/DBAppException.java \
		  DBApp.class \
		  DBAppException.class \
		  
		  

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
	
run:
	java DBApp