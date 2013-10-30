# Task for building the jar file containing all the things

JAR_FILE_V = "classifier-#{@version}-standalone.jar"

desc "Build the uberjar"
task :uberjar => [  ] do
  if `which lein`
    sh "lein uberjar"
    mv "target/#{JAR_FILE_V}", JAR_FILE
  else
    puts "You need lein on your system"
    exit 1
  end
end

