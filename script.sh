mvn clean package
if [ -d ./'Requests info' ]; then
rm -r ./'Requests info'
fi
java -jar target/lab3-1.0-SNAPSHOT-jar-with-dependencies.jar
start notepad++ ./'Requests info'/'part-00000'