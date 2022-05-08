
# GCED - Information Retrieval (Iteración 3)

En este documento se describen las tres _queries_ implementadas para la tercera iteración del *assignment* de 
la asignatura Recuperación de la Información.

## Aproximación 1. *Boolean Query with Probabilistic ReRanking* 

Se basa en utilizar la _topic query_ y parsearlo con la clase `QueryParser` sobre los _fields_ `title`, 
`abstract` y `body`. Una vez obtenidos los resultados iniciales, se asume que los _n_ primeros documentos
(ordenados por su _score_) son relevantes y se realiza _reranking_ añadiendo a la _query_ nuevos 
términos basado en el cálculo de las probabilidades de su  aparición en documentos relevantes. 
La implementación para estimar la probabilidad viene en la clase `ProbabilityFeedback` y está 
inspirada en los _odds ratio_ de 
[_Intoduction to Information Retrieval. Probabilistic information retrieval_](https://nlp.stanford.edu/IR-book/html/htmledition/probabilistic-information-retrieval-1.html).


Esta _query_ está implementada en la función `probabilisticQuery()` de la clase `QueryTopics`. 

- `n=100: MAP@k=10`: 0.45599997
- `n=1000: MAP@k=100`: 0.063999996
- `n=1000: MAP@k=1000`:  0.0127953105


## Aproximación 2. *Multifield Weighted Query* con el algoritmo KNN corregido con Rocchio

Se basa ne utilizar la función `simpleQuery()` de la segunda iteración y añadiendo la información de los *embeddings* 
con el algoritmo KNN. Con los resultados iniciales, se computa el algoritmo de Rocchio para obtener nuevos *embeddings* 
para las _queries_ y se vuelve a repetir el proceso. La implementación se encuentra en la función `knnRocchioQuery()` de 
la clase `QueryTopics`.

Los pesos asignados a la _multifield query_ de `simpleQuery()` **no** son pesos óptimos. Se escogieron por 
dar unos resultados aceptables y, en base a varios experimentos, se determinó que es favorable aumentar 
el peso del _field_ `title` a medida que _n_ y _k_ crecen.

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

- `n=100: MAP@k=10`: 0.42399997
- `n=1000: MAP@k=100`: 0.066199996
- `n=1000: MAP@k=1000`:  0.013394228


## Aproximación 3. *Multifield Weighted Query* y *Cosine Similarity* con *Page Ranking*

Para computar esta _query_ fue necesario programar lo equivalente a un *grafo de referencias* etnre los documentos 
de la colección. Esta implementación se encuentra en la clase `ObtainTransitionMatrix`, con la que se puede obtener 
la matriz de probabilidades de transición y computar el algoritmo de *page ranking* descrito en [*Introduction to 
Information Retrieval. Link analysis*](https://nlp.stanford.edu/IR-book/html/htmledition/markov-chains-1.html). 

La implementación de esta _query_ se basa en tomar los resultados iniciales de la `simpleQuery()`, computar la 
similitud coseno con los _embeddings_ de los documentos y mezclar los resultados (sumando los _scores_ y tomando los 
_n_ mejores). Posteriormente, asumiendo que los n primeros documentos con mayor _score_ son relevantes, 
utilizar _Page Ranking_ para alterar su _score_ por su ranking de página (vector _x_ descrito en el libro).

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

- `n=100: MAP@k=10`:  0.41799998
- `n=1000: MAP@k=100`: 0.058799986
- `n=1000: MAP@k=1000`:  0.0056915004
