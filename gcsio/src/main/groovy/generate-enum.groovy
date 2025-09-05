import groovy.json.JsonSlurper
import groovy.text.SimpleTemplateEngine

def jsonPath = new File(project.basedir, 'src/main/resources/feature-map.json')
def outputPath =
    new File(
        project.build.directory,
        'generated-sources/java/com/google/cloud/hadoop/gcsio/TrackedFeatures.java')
def templatePath = new File(project.basedir, 'src/main/templates/TrackedFeatures.java.tpl')

// Parse the JSON source file
def features = jsonPath.withReader("UTF-8") { reader ->
  new JsonSlurper().parse(reader).features
}

// Process the template
def template = new SimpleTemplateEngine().createTemplate(templatePath).make(["features": features])

// Write the generated Java file
outputPath.parentFile.mkdirs()
outputPath.write(template.toString(), "UTF-8")
log.info("Generated " + outputPath.getName())

// Add the generated sources to the project compile path
project.addCompileSourceRoot(new File(project.build.directory, 'generated-sources/java').path)
