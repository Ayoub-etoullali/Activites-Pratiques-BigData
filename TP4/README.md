# Exercice 1 :
![image](https://user-images.githubusercontent.com/92756846/224797797-afeeedde-1923-480e-b33e-f23071c1b312.png)

  #### <RDD 1> 
  ```sh
  SparkContext.Parallelize(Arrays.asList("ayoub hayat samira radouan ayoub ... ")
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224798123-d9ac1995-0418-4253-8fce-13bf99c8a190.png)
  
  #### <RDD 2> 
  ```sh
  rdd1.flatMap((word) -> Arrays.asList(word.split(" ")).iterator())
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224798331-aa40872b-a624-4eaa-bea0-580ec4236e97.png)

  #### <RDD 3> 
  ```sh
   rdd2.filter(nom -> {
            if(!nom.equals("ihssan")){
                return true;
            }
            return false;
        });
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224798779-4fc2cf8d-c3e0-4a12-b3b8-4377f39bb83f.png)
  
  #### <RDD 4> 
  ```sh
  rdd2.filter(nom -> {
            if(!nom.equals("ayoub")){
                return true;
            }
            return false;
        });
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224799774-cab0e6e7-bad6-45b1-998b-20a5d0672a8f.png)
  
  #### <RDD 5> 
  ```sh
  rdd2.filter(nom -> {
            if(!nom.equals("hayat")){
                return true;
            }
            return false;
        });
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224800009-d1590d98-9c9f-418a-b6fc-ec7222b5e489.png)
  
  #### <RDD 6> 
  ```sh
  rdd3.union(rdd4) 
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224800406-837201ae-2e36-4a8a-9d84-5c6bb6503a90.png)

  #### <RDD 71> 
  ```sh
  rdd5.map(noms -> noms + " ETOULLALI")
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224800763-c11a3f55-0167-447f-8ab0-5c57633cbefe.png)

  #### <RDD 7> 
  ```sh
  rdd71.mapToPair((word) -> new Tuple2<>(word, 1)) 
  rdd7.reduceByKey((a, b) -> a + b)
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224801115-a14a48f8-8f99-43f2-aa3e-f2975554e9af.png)

  #### <RDD 81> 
  ```sh
  rdd6.map(noms -> noms + " ETOULLALI")
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224801751-127f2c69-4729-4d05-8dd4-88bb59bdb769.png)

  #### <RDD 8> 
  ```sh
  rdd81.mapToPair((word) -> new Tuple2<>(word, 1))
  rdd8.reduceByKey((a, b) -> a + b)
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224802056-526fbdc5-ff7e-47a5-b63d-14e2883f2ddf.png)

  #### <RDD 9> 
  ```sh
  rdd8.union(rdd7)
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224802503-305ec920-f509-4cd9-9f6c-0af60d7c2740.png)

  #### <RDD 10> 
  ```sh
  rdd9.sortByKey()
  ```
  ![image](https://user-images.githubusercontent.com/92756846/224802646-fd806cd4-ff54-47aa-ab5f-f23b40e4898d.png)

  <br><br>
  
# Exercice 2 :
![image](https://user-images.githubusercontent.com/92756846/224802856-e9fefc64-4178-4037-b94b-8b48dfdc1439.png)
  
  #### Fichier "ventes.txt"
  ![image](https://user-images.githubusercontent.com/92756846/225772439-ea4eb6c8-1472-40a0-b109-bf214532374b.png)

  ## Question 1 :
  
  #### Transformation des lignes en tuples (ville, vente)
  ![image](https://user-images.githubusercontent.com/92756846/225772577-f64d994c-4db0-4f35-b7d6-6f093cb089b2.png)

  #### Agrégation des ventes par ville
  ![image](https://user-images.githubusercontent.com/92756846/225772704-3c76ac18-40d9-4e91-b077-a4bfcd9fbe1a.png)


  ## Question 2 : 
  
  #### Transformation des lignes en tuples (Année+ville, vente) 
  ![image](https://user-images.githubusercontent.com/92756846/225772744-38e2fffb-75ce-4d6f-b540-ccb3e1c74dac.png)

  #### Agrégation des ventes par ville pour une année
  ![image](https://user-images.githubusercontent.com/92756846/225772889-7820a24a-3fec-41cb-8798-e412c57410e8.png)

  
  <br><br>
  
# Exercice 3 :
    Nous souhaitons, dans cet exercice d’analyser les données météorologiques fournies par NCEI (National Centers for Environmental Information) à l'aide de Spark. 
    
![image](https://user-images.githubusercontent.com/92756846/224803108-f81f27f4-05d9-4b62-ab13-a531f32f8042.png)

#### Fichier "2020.csv"
![image](https://user-images.githubusercontent.com/92756846/225777933-a9c7a91c-c2e9-4b7f-b2c8-20de8097da2d.png)


