
# GCED - Information Retrieval (Iteración 3)

En este documento se describen las tres _queries_ implementadas para la tercera iteración del *assignment* de 
la asignatura Recuperación de la Información.

## Aproximación 1. *Multifield Weighted Query*

Se basa en utilizar el _topic query_ y parsearlo con la clase `QueryParser` para obtener un _score_ ponderado 
en cada _field_ de los documentos indexados. Los _fields_ usados para fueron el título (`title`), abstract (`abstract`) 
y cuerpo del texto (`body`) y sus pesos fueron respectivamente 0.5, 0.3 y 0.5. Estos pesos _no_ han sido escogidos por 
ser los óptimos, sino por ofrecer un mejor resultado para k=10. Se ha comprobado que para sacar provecho de estos pesos, 
una buena estrategia es cambiar las ponderaciones a medida que aumenta el k de la evaluación. A medida que k crece, 
es más favorable aumentar el peso del título y del abstract que el del cuerpo del artículo.

Esta _query_ está implementada en la función `simpleQuer()` de la clase `QueryTopics`. Sus resultados fueron (para los 
pesos citados):

- `MAP@k=10`: 0.44599998
- `MAP@k=100`: 0.06079999
- `MAP@k=1000`:  0.012521271


## Aproximación 2. *Multifield Weighted Query* con el algoritmo KNN corregido con Rocchio

Se basa ne utilizar la misma `simpleQuery()` de la primera aproximación y añadiendo la información de los *embeddings* 
con el algoritmo KNN. Con los resultados iniciales, se computa el algoritmo de Rocchio para obtener nuevos *embeddings* 
para las _queries_ y se vuelve a repetir el proceso. La implementación se encuentra en la función `knnRocchioQuery()` de 
la clase `QueryTopics`.

- `MAP@k=10`: 0.44599998
- `MAP@k=100`: 0.06079999
- `MAP@k=1000`:  0.012521271

Para calcular Rocchio se usa una clase adicional (`PoolRocchio`) que paraleliza el cálculo de un nuevo vector para la 
_query_ sobre todos los tópicos. Cabe desetacar que este algoritmo usa 3 parámetros que no han sido explorados (por 
tanto los valores escogidos no son los óptimos).

- `alpha`: Determina en qué magnitud se quieren tomar los valores de la _query_ inicial (se recomienda que `alpha=1`).
- `beta`: Determina en qué magnitud se quiere "saltar" al centroide de los documentos relevantes en el espacio de d 
dimensiones (donde d es el tamaño de los _embeddings_). En la literatura se recomienda un valor alto (`beta=0.75`).
- `gamma`: Determina en qué magnitu se quiere "alejar" del centroide de los documentos no relevantes en el espacio. 
En la literatura se recomiendo mantener el _recal_ y por tanto usar una posición más "conservadora" usando un valor 
pequeño (`gamma=0.1`).

**Nota**. Para utilizar Rocchio se hizo pseudo-ranking, asumiendo que los n documentos con mayor _score_ (donde n es 
el parámetro que introduce el usuario) son relevantes y el resto no lo son. Obsérvese que Rocchio asume que la 
representación vectorial de los documentos se encuentra "separada" en el espacio d-dimensional (algo bastante 
optimista). En caso de no estarlo  no mejorará los resultados.

Los resultados obtenidos fueron:

- `MAP@k=10`: 0.42399997
- `MAP@k=100`: 0.066199996
- `MAP@k=1000`:  0.013394228


## Aproximación 3. *Cosine Similarity* con *Page Ranking*

Para computar esta _query_ fue necesario programar lo equivalente a un *grafo de referencias* etnre los documentos 
de la colección. Esta implementación se encuentra en la clase `ObtainTransitionMatrix`, con la que se puede obtener 
la matriz de probabilidades de transición y computar el algoritmo de *page ranking* descrito en [*Introduction to 
Information Retrieval. Link analysis*](https://nlp.stanford.edu/IR-book/html/htmledition/markov-chains-1.html). 

La implementación de esta _query_ se basa en computar la similitud coseno con los _embeddings_ de los documentos y 
posterirmente, asumiendo que los n primeros documentos con mayor _score_ son relevantes, utilizar _Page Ranking_ para 
alterar su _score_ por su ranking de página (vector _x_ descrito en el libro).

Los parámetros que se usan en esta aproximación son:

- `alpha`: Probabilidad de saltar aleatoriamente a un documento a la hora de construir la matriz de transición. Por 
defecto se utiliza `alpha=0` para obtener una rápida convergencia y que la matriz de transiciones recoja únicamente 
las relaciones entre documentos, sin introducir componente aleatoria. Este parámetro *no* es óptimo, puede ser 
cambiado para estudiar su comportamiento en futuras iteraciones.
- `iterations`: Número de iteraciones máximas para la convergencia de la matriz de transición. Cuando el vector _x_ 
converge, el algoritmo se detiene. Si la convergencia tarda mucho, se fija un número de iteraciones máximo. Por 
defecto, `iterations=1000` (se puede alterar pero se recomienda un valor medio para evitar la sobrecarga de recursos).

**Nota**: A la hora de cambiar el _score_ de los documentos según su Page Rank, se utiliza la siguiente fórmula:

$$ \text{PageRank} \cdot \text{initialScore} + \text{initialScore} $$

De esta forma, los resultados obtenidos tendrán en cuenta tanto la similitud coseno de los documentos con las _queries_ 
como el _page rank_ de los documentos.

**Nota**: Esta aproximación es computacionalmente costosa, pues se tiene que crear el grafo de referencias entre los 
_n_ documentos obtenidos. Si _n_ es muy grande, el tiempo de cómputo será muy elevado.

Los resultados obtenidos fueron:

- `MAP@k=10`: 0.42399997
- `MAP@k=100`: 0.066199996
- `MAP@k=1000`:  0.013394228
