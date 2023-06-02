# TP Sqoop

## Installer Sqoop

▪ Configurez les variables d'environnement en modifiant le fichier .bashrc à
l'aide d'un éditeur de texte. Exécutez la commande suivante pour ouvrir le
fichier dans l'éditeur nano :
```
nano ~/.bashrc
```

▪ Faites défiler jusqu'à la fin du fichier et ajoutez les lignes suivantes :
```
export SQOOP_HOME=/opt/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
```

▪ Enregistrez le fichier et quittez l'éditeur de texte.
▪ Chargez les variables d'environnement mises à jour en exécutant la commande
suivante :
```
source ~/.bashrc
```


▪ Vérifiez l'installation en exécutant la commande suivante :
```
sqoop version
```

## Importer et exporter vers mysql

▪ Importer vers HDFS à paratir de mysql:
```
sqoop import --connect jdbc:mysql://localhost/DB_Spark --username "root" --password ""
--table EMPLOYES --target-dir /sqoop
```

▪ Exporter vers mysql à partir de hdfs
```
sqoop export --connect jdbc:mysql://localhost/test_sqoop --username your_username --password
your_password --table employees --export-dir /sqoop/employees_data --input-fields-terminated-by
',' --input-lines-terminated-by '\n'
```

#### Demo :
<div align="center">
       <p>
       <sup>  <strong>Vidéo -</strong> TP Sqoop</sup>
       </p>
</div>

<kbd>Enjoy Code</kbd> 👨‍💻
