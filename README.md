<!--
 This project is free software; you can redistribute it and/or modify it
 under the terms of the GNU General Public License as published by the
 Free Software Foundation; only version 2 of the License, dated June 1991.
 This program is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
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
