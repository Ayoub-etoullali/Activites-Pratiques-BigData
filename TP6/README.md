# TP5: Spark SQL

## Enoncé : 
L’hôpital national souhaite traiter ces données au moyen d’une application Spark d’une manière parallèle est distribuée. L’hôpital possède des données stockées dans une base de données relationnel et des fichiers csv. L’objectif est de traiter ces données en utilisant Spark SQL à travers les APIs DataFrame et Dataset pour extraire des informations utiles afin de prendre des décisions.
***

  ### I. Traitement de données stockées dans Mysql
  L’hôpital possède une application web pour gérer les consultations de ces patients, les données sont stockées dans une base de données MYSQL nommée DB_HOPITAL, qui contient trois tables PATIENTS, MEDECINS et CONSULTATIONS.
  #### Travail à faire :
  Vous créez la base de données et les tables et vous répondez aux questions suivantes :
  
  1. Afficher le nombre de consultations par jour.

  2. Afficher le nombre de consultation par médecin. Le format d’affichage est le suivant : NOM | PRENOM | NOMBRE DE CONSULTATION

  3. Afficher pour chaque médecin, le nombre de patients qu’il a assisté.

#### Demo :
https://user-images.githubusercontent.com/92756846/230735909-02098aa9-02d4-4c86-b52f-b21edc2d135d.mp4
<div align="center">
       <p>
       <sup>  <strong>Vidéo -</strong>  Spark SQL</sup>
       </p>
</div>

  ### II. Traitement de données en streaming.
  On souhaite développer pour l’hôpital une application Spark qui reçois les incidents de
  l’hôpital en streaming avec Structured Streaming. Les incidents sont reçu en streaming dans
  des fichiers csv (voir le fichier en pièce jointe).
  Le format de données dans les fichiers csv et la suivante :
  Id, titre, description, service, date
  #### Travail à faire :
  1. Afficher d’une manière continue le nombre d’incidents par service.

  2. Afficher d’une manière continue les deux année ou il a y avait plus d’incidents.

<kbd>Enjoy Code</kbd> 👨‍💻
