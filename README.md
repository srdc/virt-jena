<!--
  The SALUS Project licenses the semanticMDR project with all of its source files 
  to You under the Apache License, Version 2.0 (the "License"); you may not use 
  this file except in compliance with the License.  
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Virtuoso Jena Provider
===========

The Virtuoso Jena RDF Data Provider is a fully operational Native Graph Model Storage Provider for the Jena Framework, which enables Semantic Web applications written using the Jena RDF Frameworks to directly query the Virtuoso RDF Quad Store. More information and source of  project can be found in

	http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtJenaProvider
	
However, current release (2.6.2) of the Virtuoso Jena Provider is designed to work with Virtuoso JDBC 3 Driver and Jena versions above 2.5.5
Current release supports neither Virtuoso JDBC 4 Driver nor Jena version 2.6.4 above, where Jena Framework became a top level Apache Project.
This project includes modifications on Virtuoso Jena Provider 2.6.2 so that, it can work with Virtuoso JDBC 4 Driver and current latest Jena release 2.10.0

## Installation
===========

Apache Maven is required to build the virt-jena. Please visit
http://maven.apache.org/ in order to install Maven on your system.

Under the root directory of the semanticMDR project run the following:

	$ virt-jena> mvn install

In order to make a clean install run the following:

	$ virt-jena> mvn clean install
	
## Usage
===========

After building the project, *virt-jena-2.6.2-srdc.jar* is found under *target* directory. To use the Virtuoso Jena provider,
archive file can be directly added as dependency, or can be specified as maven dependecny by adding following to *pom.xml*.

	<dependency>
		<groupId>tr.com.srdc</groupId>
		<artifactId>virt-jena</artifactId>
		<version>2.6.2-srdc</version>
    </dependency>
	
since above mentioned installation process deploys Virtuoso Jena Provider to local maven repository.