# Steel-EYE assignment
<strong>As a part of SteelEYE DataEngineer assessment this is built</strong>
<ul>
<li>zipdownloader.py -> this file/module contains a method that can take an xml file containing download-link under result tag as input</li>
<li>dataextractor.py -> this file/module contains futher steps that need to be done in a class called DataExtractor that<ul> <li>unzips the zip downloaded from zipdownloader </li><li>extracts the required attributes from the xml</li><li>stores them in csv</li><li>Uploads to S3 bucket</li></ul></li>
</ul>
<strong>In further update the zipdownloader module methods have been merged in the DataExtractor to directly access initial xml file and access all the other in 1 single command</strong>

<p>example code:</p>
<code>python dataextractor.py downloadedxml.xml</code>
