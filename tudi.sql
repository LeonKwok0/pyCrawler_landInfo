
CREATE DATABASE IF NOT EXISTS LandInfo  DEFAULT CHARACTER SET utf8


SELECT * FROM LandInfo.NanJing 

CREATE TABLE IF NOT EXISTS LandInfo.NanJing( 
FloorOrder  VARCHAR(45)  NOT NULL,   
name  VARCHAR(45) NULL,    
address  VARCHAR(255) NULL,    
size  VARCHAR(45) NULL,  
FARatio  VARCHAR(45) NULL,   
soldDate  VARCHAR(45) NULL,    
price  VARCHAR(45) NULL,    
owner  VARCHAR(45) NULL,   
PRIMARY KEY ( FloorOrder));

DROP DATABASE LandInfo

show tables from LandInfo

