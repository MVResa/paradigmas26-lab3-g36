## Ejercicio 1A
Diagrama de flujo
```mermaid
flowchart TB

A(Leer Suscripcion) 
B(Filtrar suscripciones malformadas)
C(Descargar feed y parsear post)
D(Filtrar posts vacios)
E(Detectar entidades)
F(Contamos las entidades)
G(Rankear y mostrar resultados)
H(Cargar diccionarios)

A --> |"List[Option[Subscription]]"| B
B --> |"List[Subscription]"| C
C-->|"List[Post]"|D
D-->|"List[Post]"|E
E-->|"List[NamedEntity]"|F
H-->|"Dictionary"|E
F-->|"Map[(EntityType, String), Int]"|G
```
## Ejercicio 1B
Pasos del pipeline que pueden ser expresados con las abstracciones de Spark:
A-->B No hay abstraccion, leer las subs lo hace el driver 
B-->C Se utiliza filter y con esto elimino las listas malformadas None, queda asi List(sub1, sub2, sub3).
C-->D Para esto usamos flatMap.
D-->E Se usa filter para filtrar los posts vacios.
E-->F Detectar entidades se puede escribir con flatMap porque por cada post se pueden detectar varias entidades.
F-->G para contar entidades podemos usar reduceByKey porque lo que haria es hagarrar todos los elementos que tengan el mismo nombre y devovleria la cantidad.
Para rankear y mostrar resultados no hay ninguna abstraccion porque los drivers se encargan de eso. Lo mismo con la carga de diccionarios. 

## Ejecicio 1C
Pasos del pipeline son barreras y no pueden ejecutarse de forma independiente:
Contar entidades (reduceByKey) es una barrera de sincronización porque para saber cuántas veces aparece cada entidad necesitás que todos los workers hayan terminado de detectar entidades. Ningún worker puede dar el conteo final hasta que todos hayan procesado sus posts.
Pasos del pipeline que pueden ejecutarse de forma completamente independiente entre workers:
Todos excepto contar entidades porque dependemos de que tengamos la lista completa de entidades.

## Ejercicio 1D 
Las funciones que se le pasan a Spark deben poder serializarse para viajar por la red a los workers. No pueden depender de estado compartido porque cada worker trabaja de forma independiente. Y deben evitar efectos secundarios porque Spark puede reejecutar una función si un worker falla.

## Ejercicio 2 
En caso de propagarse las excepciones dentro de un worker, el driver fallaria e intentaria relanzar la tarea hasta un maximo de *spark.task.maxFailures* , si todos los intentos de llevar a cabo la tarea fallaran el sistema colapsaria, por eso cada worker debe tener un manejar las excepciones que se podrian presentar de manera independiente.

## Ejercicio 3
reduceByKey es una barrera de sincronización porque ningún worker puede calcular el total final de entidades hasta que todos los workers terminen de trabajar y envien sus datos, sino los datos estarian incompletos.
La restricción que tiene la función que se le pasa a reduceByKey es que trabaja sumando 2 valores, al ser asociativa y conmutativa puede sumar los resultados de distintos workers sin importar el orden.
La lectura del diccionario se hace en el driver asi Spark lo serializa y se lo envia a los workers.