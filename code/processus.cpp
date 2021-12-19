// Compilation : 
// g++ -c processus.cpp && g++ calculCC.o processus.o -o processus -lpthread

// TODO : - Utiliser uniquement des sockaddr_in
//        - Enlever le plus de paramètres possible pour l'execution
//        - Simplifier le code
//        - Variables conditionnelles à la place des SEM

// Naimi Asus  :
// ./processus 1 192.168.1.65 6001 192.168.1.65 6001
// ./processus 2 192.168.1.65 6002 192.168.1.65 6001
// ./processus 3 192.168.1.65 6003 192.168.1.65 6001

// Naimi Asus ROG avec une grosse carte graphique :
// ./processus 1 172.29.179.149 6001 172.29.179.149 6001
// ./processus 2 172.29.179.149 6002 172.29.179.149 6001
// ./processus 3 172.29.179.149 6003 172.29.179.149 6001
// ./processus 4 172.20.176.117 6004 172.20.176.117 6001


// TODO
// Envoyer un int pour le port
// Enoyer des sockadress à la place des char*
// Variables conditionnelles à la place des sémaphores
// Gestion des erreurs

#include <vector>
#include <iostream>
#include <string>
#include <time.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <ctime>
#include "calcul.h"
#include <chrono>

#define MAX_BUFFER_SIZE 16000

struct paramsFonctionThread {
  int idThread;
  int idSEM; // A enlever 
  int idSHM; 
  
};

struct commonData{ 
  bool token; // Voir si c'est utile ? 
  bool demande; 
  
  /*SUPP*/
  char* ip;
  char* port;
  char* ipSuivant;
  char* portSuivant; 
  char* ipPere;
  char* portPere; 
  /*SUPP*/

  //sockaddr_in pere; 
  //sockaddr_in suivant; 


}SHM;

std::string getTimeStr(){
    std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string s(30, '\0');
    std::strftime(&s[0], s.size(), "%H:%M:%S", std::localtime(&now));
    return s;
}

void sendMessageTo(char* msg, sockaddr_in& adrServ){
  //std::cout<<"ip : "<<ip<<" port : "<<port<<std::endl;
  // ----------------------- EMETTEUR 
  int ds = socket(PF_INET, SOCK_STREAM, 0);

  if (ds == -1) {
    printf("Client : pb creation socket\n");
    exit(1); 
  }

  // struct sockaddr_in adrServ;
  // adrServ.sin_addr.s_addr = inet_addr(ip);
  // adrServ.sin_family = AF_INET;
  // adrServ.sin_port = htons(atoi(port));
  socklen_t lgAdr = sizeof(struct sockaddr_in);

  int conn = -1;

  if(conn == -1){
    conn = connect(ds,(struct sockaddr*) &adrServ, lgAdr);
    if (conn <0) {
      perror ("Client: pb au connect :");
      close (ds); 
      exit (1); 
    }
  }


  // Envoie de la taille 
  int nom_size = strlen(msg) + 1;
 
  int snd = send(ds, (char*)&nom_size, sizeof(nom_size),0);
  if (snd == -1) {
    printf("Client : send n'a pas fonctionné\n");
  }


    // Envoie du mot clé 
  snd = send(ds, (char*)msg, nom_size, 0);
  if (snd == -1) {
    printf("Client : send n'a pas fonctionné\n");
  }

  close (ds);
  shutdown(ds, SHUT_WR); 
}

// Appel dans les autres fonctions
// Permet d'envoyer un message à un autre processus
// avec une socket et un appel avec send
void sendMessageTo(char* msg, char* ip, char* port){
  //std::cout<<"ip : "<<ip<<" port : "<<port<<std::endl;
  // ----------------------- EMETTEUR 
  int ds = socket(PF_INET, SOCK_STREAM, 0);

  if (ds == -1) {
    printf("Client : pb creation socket\n");
    exit(1); 
  }

  struct sockaddr_in adrServ;
  adrServ.sin_addr.s_addr = inet_addr(ip);
  adrServ.sin_family = AF_INET;
  adrServ.sin_port = htons(atoi(port));
  socklen_t lgAdr = sizeof(struct sockaddr_in);

  int conn = -1;

  if(conn == -1){
    conn = connect(ds,(struct sockaddr*) &adrServ, lgAdr);
    if (conn <0) {
      perror ("Client: pb au connect :");
      close (ds); 
      exit (1); 
    }
  }


  // Envoie de la taille 
  int nom_size = strlen(msg) + 1;
 
  int snd = send(ds, (char*)&nom_size, sizeof(nom_size),0);
  if (snd == -1) {
    printf("Client : send n'a pas fonctionné\n");
  }


    // Envoie du mot clé 
  snd = send(ds, (char*)msg, nom_size, 0);
  if (snd == -1) {
    printf("Client : send n'a pas fonctionné\n");
  }

  close (ds);
  shutdown(ds, SHUT_WR); 
}

// Appel dans le main
// Création du thread receveur
// Boucle d'attente de réception du token
void * fonctionThreadReceveur (void * params){
  struct paramsFonctionThread * args = (struct paramsFonctionThread *) params;

  int ds = socket(PF_INET, SOCK_STREAM, 0);

  if (ds == -1) {
    perror("Serveur : probleme creation socket\n");
    exit(1); 
  }

  // Attachement 
  struct commonData * p_att;

  p_att = (commonData *)shmat(args->idSHM, NULL, 0); 

  if((void *)p_att == (void *)-1){
    perror("shmat");
  }


  struct sockaddr_in server;
  server.sin_family = AF_INET;
  server.sin_addr.s_addr = INADDR_ANY;
  server.sin_port = htons(atoi(p_att->port));

  if(bind(ds, (struct sockaddr*)&server, sizeof(server)) < 0) {
    perror("Serveur : erreur bind");
    close(ds); 
    exit(1); 
  }

  //printf("Serveur : nommage : ok\n");
  int ecoute = listen(ds, 10);
  if (ecoute < 0) {
    printf("Serveur : je suis sourd(e)\n");
    close (ds);
    exit (1);
  }
  
  char messagesRecus[MAX_BUFFER_SIZE];
  std::string DEMANDE_RECUS = "D";
  std::string TOKEN_RECUS = "T";
  int name_size = 0;
  int rcv = 0;
  int dsCv = 0;
  
  fd_set set;
  fd_set settmp;
  FD_ZERO(&set);
  FD_SET(ds, &set);
  
  int max = ds;
  struct sockaddr_in addrC;
  socklen_t lgCv = sizeof(struct sockaddr_in);

  // Structure pour les opérations sur les SEM
  struct sembuf opp;

  /* boucle de traitement des messages recus */
  while(1){

    settmp = set;
    select(max+1, &settmp, NULL, NULL, NULL);

    // Accepter les message recus : 
    dsCv = accept(ds, (struct sockaddr *)&addrC, &lgCv);

    // addrC : Contient les infos sur le client
    // On peut connaitre l'adresse ip et la prot du client

    // ??  
    FD_SET(dsCv, &set);
    if( max < dsCv) {
      max = dsCv;
    }
    
    // Reception de la taille du message
    rcv = recv(dsCv, &name_size, sizeof(int), 0);

    // Reception du message 
    rcv = recv(dsCv, messagesRecus, name_size,0);

    std::string msgRCV(messagesRecus); 

    // Prendre le verrou
    opp.sem_num = 0; // Numéro du sémaphore
    opp.sem_op = -1; // Opération 
    
    semop(args->idSEM, &opp, 1);

    if(msgRCV.at(0) == DEMANDE_RECUS.at(0)){
      // ------------------------------------------------------
      //        RECEPTION Req(k)(k est le demandeur)
      // ------------------------------------------------------


      // On récupère l'ip et le port de K : 
      std::vector<std::string> tab(3, ""); 
      std::string delimiter = "/";

      size_t pos = 0;
      int i = 0; 
      std::string token;
      while ((pos = msgRCV.find(delimiter)) != std::string::npos) {
        token = msgRCV.substr(0, pos);
        tab[i] = token;
        msgRCV.erase(0, pos + delimiter.length());
        i++; 
      }

      char *ipK = new char[tab[1].length() + 1];
      strcpy(ipK, tab[1].c_str());

      char *portK = new char[tab[2].length() + 1];
      strcpy(portK, tab[2].c_str());

      // si père = "" 
      if(std::string(p_att->ipPere) == std::string("") && std::string(p_att->portPere) == std::string("")){
          std::cout<<"Demande recus !"<<std::endl; 
          // si demande 
          if(p_att->demande == true){
            // suivant := k 
            p_att->ipSuivant = ipK;    // ip de K
            p_att->portSuivant = portK;   // port de K; 
          }
          // sinon
          else{
            // jeton-présent := faux; 
            p_att->token = false; 
            // envoyer token à k 
            char* tok = strdup("T"); 
            sendMessageTo(tok, ipK, portK);

          } 
        
      }// sinon 
      else {
        // envoyer req(k) à père 
        std::string msgTMP = "D/" + std::string(ipK) + "/" + std::string(portK)+ "/"; 

        char *m = new char[msgTMP.length() + 1];
        strcpy(m, msgTMP.c_str());

        sendMessageTo(m, p_att->ipPere, p_att->portPere);

      }
      // père := k 
      p_att->ipPere = ipK; 
      p_att->portPere = portK; 
      
    }
    else if(msgRCV.at(0) == TOKEN_RECUS.at(0)){
      // ------------------------------------------------------
      //                    RECEPTION TOKEN
      // ------------------------------------------------------
      std::cout<<"Token recus : "<<getTimeStr()<<std::endl;
      

      // jeton-présent := vrai 
      p_att->token = true; 

      // Je libère le sémaphore
      opp.sem_num = 1;
      opp.sem_op = 1; 
      
      semop(args->idSEM, &opp, 1);
    }
    else{
      //Afficher le message recus :
      std::cout<<"Message recus => "<<msgRCV<<std::endl;
    }

    // Rend le verou
    opp.sem_num = 0;
    opp.sem_op = 1; 
    //opp.sem_flg = 0; 
    semop(args->idSEM, &opp, 1);

  }

  close (ds); // atteignable si on sort de la boucle infinie.
  std::cout<<"Serveur : fin"<<std::endl; 

  pthread_exit(NULL);
  return 0; 
}

// Appel dans le main
// Prend le token, rentre en section critique 
// et passe le token au suivant
void* fonctionThreadEmetteur (void * params){
  struct paramsFonctionThread * args = (struct paramsFonctionThread *) params;

  char* msg = strdup("send"); 

  srand(time(NULL));

  // Attachement 
  struct commonData * p_att;

  p_att = (commonData *)shmat(args->idSHM, NULL, 0); 

  if((void *)p_att == (void *)-1){
    perror("shmat");
  }

  // Structure pour les opérations sur les SEM
  struct sembuf opp;

  while(1){
    // ------------------------------------------------------
    //                        CALCUL
    // ------------------------------------------------------
    calcul(rand()%4+1); 

    // ------------------------------------------------------
    //          DEMANDE D'ENTRER EN SECTION CRITIQUE
    // ------------------------------------------------------

    // Prendre la verrou
    opp.sem_num = 0; // Numéro du sémaphore
    opp.sem_op = -1; // Opération 
    
    semop(args->idSEM, &opp, 1);
    //std::cout<<"Emetteur : Verrou prit pour faire la demande"<<std::endl;

    // demande = true;  
    p_att->demande = true; 

    // si pere == "" 
    if(std::string(p_att->ipPere) == std::string("") && std::string(p_att->portPere) == std::string("")){
      // alors entrée en section critique

    }
    // sinon 
    else{
      //   début envoyer Req(i) à père;
      std::string msgTMP = "D/" + std::string(p_att->ip) + "/" + std::string(p_att->port)+ "/"; 

      char *m = new char[msgTMP.length() + 1];
      strcpy(m, msgTMP.c_str());

      sendMessageTo(m, p_att->ipPere, p_att->portPere); 

      //   père = "" 
      p_att->ipPere = strdup(""); 
      p_att->portPere = strdup("");

    }

    //std::cout<<"Emetteur : Demande faite je rend le verrou"<<std::endl;
    // Rend le verou
    opp.sem_num = 0;
    opp.sem_op = 1; 
    //opp.sem_flg = 0; 
    semop(args->idSEM, &opp, 1);

    // J'attend le token
    opp.sem_num = 1;
    opp.sem_op = -1; 
     
    std::cout<<"Emetteur : J'attend le token"<<std::endl;

    semop(args->idSEM, &opp, 1); // A Remplacer par des variables conditionnelles
    std::cout<<"Emetteur : J'ai le token ! je rentre en section critique. "<<getTimeStr()<<std::endl;

    // ------------------------------------------------------
    //              ENTRER EN SECTION CRITIQUE
    // ------------------------------------------------------
    // Calcule dans la section critique 
    calcul(rand()%4+1); 
    std::cout<<"Emetteur : Section critique fini"<<std::endl;
    // ------------------------------------------------------
    //              LIBERATION DE LA RESOURCE
    // ------------------------------------------------------
    // Prendre la verrou
    opp.sem_num = 0; // Numéro du sémaphore
    opp.sem_op = -1; // Opération 
    //opp.sem_flg = 0; // ??? 
    semop(args->idSEM, &opp, 1);
    std::cout<<"Emetteur : Verrou prit pour faire la liberation"<<std::endl;

    // demande = false;
    p_att->demande = false; 

    // si suivant != "" 
    if(std::string(p_att->ipSuivant) != std::string("") && std::string(p_att->portSuivant) != std::string("")){
      //   envoyer token à suivant;
      std::cout<<"Emetteur : Je passe le jeton au suivant"<<std::endl;
      //sendToken(); 
      char* m = strdup("T"); 
      sendMessageTo(m, p_att->ipSuivant, p_att->portSuivant); 
      //   jeton-présent := faux;
      p_att->token = false; 
      //   suivant := nil 
      p_att->ipSuivant = strdup("");
      p_att->portSuivant = strdup(""); 
    }

    // Rend le verou
    opp.sem_num = 0;
    opp.sem_op = 1; 
     
    semop(args->idSEM, &opp, 1);
    std::cout<<"Emetteur : Liberation faite je rend le verrou"<<std::endl;
  }
}


// Création des sémaphores, initialisation, création des threads
int main(int argc, char * argv[]){

  // Vérifier les paramètres
  if (argc < 6){
    printf("utilisation: %s  id ip port pere_ip pere_port \n", argv[0]);
    return 1;
  }   

  char * ipProcessus = argv[2]; 
  char * portProcessus = argv[3]; 
  char * ipPere = argv[4];
  char * portPere = argv[5];


  // ----------------------- SEMAPHORE ----------------
  int valeurInit = 1; 
  char* pourCle = strdup("pourCle.txt"); 
  int entierPourCle = atoi(argv[1]); 
  
  int cleSEM = ftok(pourCle, entierPourCle);

  int nbSem = 2;
  
  // On essaie de se connecter au tableau semaphores
  int idSEM = semget(cleSEM, nbSem, IPC_EXCL | 0666);
  
  // Si il existe pas on le crée
  if(idSEM == -1){
    //Création du tableau de sémaphores
    idSEM = semget(cleSEM, nbSem, IPC_CREAT | IPC_EXCL | 0666);
    // Vérifier si le tableau à bien été créé
    if(idSEM == -1){
      perror("erreur semget : ");
      exit(-1);
    }
  }


  // initialisation des sémaphores a la valeur passée en parametre (faire autrement pour des valeurs différentes):
  ushort tabinit[nbSem];

  tabinit[0] = 1; 
  tabinit[1] = (std::string(ipPere) == std::string(ipProcessus) && std::string(portPere) == std::string(portProcessus)) ? 1 : 0;  
 

  union semun{
    int val;
    struct semid_ds * buf;
    ushort * array;
  } valinit;
  
  valinit.array = tabinit;

  if (semctl(idSEM, nbSem, SETALL, valinit) == -1){
    perror("erreur initialisation sem : ");
    exit(1);
  }

  /* test affichage valeurs des sémaphores du tableau */
  valinit.array = (ushort*)malloc(nbSem * sizeof(ushort)); // pour montrer qu'on récupère bien un nouveau tableau dans la suite

  if (semctl(idSEM, nbSem, GETALL, valinit) == -1){
    perror("erreur initialisation sem : ");
    exit(1);
  } 
   
  printf("Valeurs des sempahores apres initialisation [ "); 
  for(int i=0; i < nbSem-1; i++){
    printf("%d, ", valinit.array[i]);
  }
  printf("%d ] \n", valinit.array[nbSem-1]);

  free(valinit.array);

  // ----------------------- SHM 
  int clesSHM = ftok(pourCle, entierPourCle);

  int idSHM = shmget(clesSHM, size_t(sizeof(SHM)) , IPC_CREAT | 0666);
  
  if(idSHM == -1){
    perror("erreur semget : ");
    exit(-1);
  }

  struct commonData * p_att;

  p_att = (commonData *)shmat(idSHM, NULL, 0); 

  if((void *)p_att == (void *)-1){
    perror("shmat");
  }

  p_att->ip = ipProcessus; 
  p_att->port = portProcessus; 

  // ----------------------- Initialisation
  // père := 1
  p_att->ipPere = ipPere; 
  p_att->portPere = portPere; 

  // suivant := nil 
  p_att->ipSuivant = strdup(""); 
  p_att->portSuivant = strdup(""); 
  
  // demande := faux 
  p_att->demande = false;

  // si père = i // Soit meme
  if(std::string(ipPere) == std::string(ipProcessus) && std::string(portPere) == std::string(portProcessus)) {
    std::cout<<"Je commence !"<<std::endl; 
    //   alors debut jeton-présent := vrai;
    p_att->token = true; 
    //   père := null 
    p_att->ipPere = strdup(""); 
    p_att->portPere = strdup(""); 
  }
  // sinon 
  else {
    std::cout<<"Je commence pas !"<<std::endl; 
    // jeton-présent :=faux 
    p_att->token = false;
  }


  // -------------- Création des deux threads Emetteur/Receveur
  pthread_t threadReceveur;
  pthread_t threadEmetteur;

  // Déclaration des structures pour passer les paramètres aux deux threads
  struct paramsFonctionThread params; 

  // Allocation des variables pour les paramètres du Receveur
  params.idThread = 1; 
  params.idSEM = idSEM; 
  params.idSHM = idSHM; 

  // Création du thread Receveur 
  if (pthread_create(&threadReceveur, NULL, fonctionThreadReceveur, &params) != 0){
    perror("erreur creation thread receveur");
    exit(1);
  }

  // Création du thread Emetteur
  if (pthread_create(&threadEmetteur, NULL, fonctionThreadEmetteur, &params) != 0){
    perror("erreur creation thread emetteur");
    exit(1);
  }

  pthread_join(threadReceveur, NULL); 

  return 0;

}