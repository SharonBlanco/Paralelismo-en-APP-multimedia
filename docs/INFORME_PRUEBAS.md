**Informe de pruebas: guía y plantilla**  
Este documento lista las pruebas que demuestran lo que pide el enunciado: que el sistema aguanta carga, que el trabajo se reparte de verdad entre computadoras, que maneja casos mezclados y cómo se comporta cuando algo falla.  
Cada prueba tiene cuatro partes:  
- **Qué demuestra**  
- **Cómo se hace** (los comandos)  
- **Qué capturar** (pantallazos y archivos para el informe)  
- **Tabla** para anotar los resultados  
*Las tablas están vacías a propósito: se llenan con lo que pase en las * ***tres computadoras reales*** *. Los comandos se corren desde la carpeta del proyecto, con el entorno activado (* *source .venv/bin/activate* *).*  
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANUlEQVR4nO3OMQ2AABAAsSNBCUpfEJ5YGBDBgAU2QtIq6DIzW7UHAMBfHGt1V+fXEwAAXrseHDYF+yOk59sAAAAASUVORK5CYII=)  
**Antes de empezar**  
1. Encender el coordinador y los tres workers, cada uno en su computadora (ver README).  
2. En el dashboard, revisar que diga **"3 workers conectados en 3 máquinas distintas"** y que los tres estén en  **v4**.  
3. Tener el dataset generado (python scripts/descargar_dataset.py). Queda en dataset_real/.  
4. Anotar la composición del dataset. Los datos están en dataset_real/composicion.json y, en gráficos, en la sección **Variedad de archivos recibidos** del dashboard (sacar captura).  
| | |  
|-|-|  
| **Dato** | **Valor** |   
| Archivos totales |   |   
| Casos homogéneos / heterogéneos |   |   
| Videos / audios / imágenes |   |   
| Livianos / medianos / pesados / muy pesados (400–600 MB) |   |   
| Volumen total (GB) |   |   
   
1. Anotar las computadoras. Los datos salen de la tabla de Workers del dashboard (columnas IP y Máquina):  
| | | | | | |  
|-|-|-|-|-|-|  
| **Rol** | **Nombre / IP** | **Procesador** | **Núcleos** | **RAM** | **Sistema** |   
| Coordinador + worker-1 |   |   |   |   |   |   
| worker-2 |   |   |   |   |   |   
| worker-3 |   |   |   |   |   |   
| Cómo se conectan (Wi-Fi, Tailscale…) |   |   |   |   |   |   
   
**En todas las pruebas conviene guardar:** capturas del dashboard (workers, colas, trazabilidad) y los archivos .csv y .json que genera --metricas en la carpeta pruebas/.  
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANklEQVR4nO3OQQmAABRAsSeYxZw/lVeDGMACBrCCNxG2BFtmZquOAAD4i3Ot7mr/egIAwGvXA6fOBdd+dKAKAAAAAElFTkSuQmCC)  
**P1. El trabajo se reparte entre computadoras distintas**  
**Qué demuestra:** que no todo corre en una sola máquina.  
**Cómo:**  
python cliente/cliente.py carga ./dataset_real --concurrentes 4 --metricas pruebas/p1_carga_completa.csv  
   
**Qué capturar:** la tabla de Workers (IPs y máquinas distintas, columna  **Procesadas**), la línea de tiempo con barras superpuestas y el "Reparto por worker" que imprime el cliente al final.  
| | | | | | |  
|-|-|-|-|-|-|  
| **Worker** | **IP** | **Tareas hechas** | **% del total** | **Tiempo trabajando (s)** | **Fallidas** |   
| worker-1 |   |   |   |   |   |   
| worker-2 |   |   |   |   |   |   
| worker-3 |   |   |   |   |   |   
   
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANUlEQVR4nO3OQQ2AQBAAsSE5CbzRujLwhwQMYIEfIWkVdJuZozoDAOAvrlWtav96AgDAa/cDEXQEKquakOYAAAAASUVORK5CYII=)  
**P2. Más computadoras = más rápido**  
**Qué demuestra:** cuánto se acelera el trabajo al sumar workers.  
**Cómo:** mandar los mismos casos tres veces: con 1 worker encendido, con 2 y con 3. Entre corridas se pueden borrar los casos.  
python cliente/cliente.py carga ./dataset_real --filtro audio_conversion --concurrentes 4 --metricas pruebas/p2_1worker.csv  
 # encender otro worker y repetir con p2_2workers.csv; después con los 3 y p2_3workers.csv  
   
| | | | | |  
|-|-|-|-|-|  
| **Workers** | **Tiempo total (s)** | **Archivos por minuto** | **Speedup (del resumen)** | **Cuántas veces más rápido que con 1** |   
| 1 |   |   |   | 1.0× |   
| 2 |   |   |   |   |   
| 3 |   |   |   |   |   
   
**Para comentar:** por qué no es exactamente 2 o 3 veces más rápido: el tiempo de subir y bajar archivos por la red, computadoras de distinta potencia, o una tarea muy larga que hace esperar al resto.  
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANklEQVR4nO3OUQmAABBAsSeYxZyXSzCJASxgACv4J8KWYMvMbNURAAB/ca7VXe1fTwAAeO16AKe+BdmJqrPdAAAAAElFTkSuQmCC)  
**P3. Casos mezclados (heterogéneos)**  
**Qué demuestra:** que un mismo caso genera operaciones distintas según el tipo de archivo.  
**Cómo:**  
python cliente/cliente.py enviar ./dataset_real/caso_022_heterogeneo  
 python cliente/cliente.py estado case-xxxxxxxx      # el número que devolvió el comando anterior  
   
**Qué capturar:** el detalle del caso en el dashboard (columnas Operación y Pool), el reporte consolidado y el mapa de flujo.  
| | | | | | |  
|-|-|-|-|-|-|  
| **Archivo** | **Tipo** | **Operaciones que se le hicieron** | **Cola** | **Worker** | **Resultado** |   
|   |   |   |   |   |   |   
   
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANUlEQVR4nO3OYQ1AABSAwY9JoICqL4Z8Ikiggn9mu0twy8wc1RkAAH9xbdVa7V9PAAB47X4A9CgEJQFjJ/EAAAAASUVORK5CYII=)  
**P4. Workers genéricos contra especializados (Unidad 1)**  
**Qué demuestra:** respalda con datos la decisión de que todos los workers hagan de todo (ver Arquitectura, sección 4).  
**Cómo:** mandar la misma carga con dos configuraciones.  
- **A, genéricos:** los tres workers normales.  
- **B, especializados:** la computadora más potente solo con video (-e WORKER_POOLS=video -e WORKER_CONCURRENCY=2) y las otras dos con audio y ligera (-e WORKER_POOLS=audio,ligera).  
python cliente/cliente.py carga ./dataset_real --max-casos 15 --concurrentes 4 --metricas pruebas/p4_genericos.csv  
 # cambiar la configuración de los workers y repetir con p4_especializados.csv  
   
| | | | |  
|-|-|-|-|  
| **Configuración** | **Tiempo total (s)** | **Duración promedio de un caso (s)** | **Speedup** |   
| A, genéricos |   |   |   |   
| B, especializados |   |   |   |   
   
**Para comentar:** ¿cuál terminó antes? En B, ¿hubo computadoras sin nada que hacer mientras otra cola estaba llena? ¿Qué pasaría en B si se apaga la computadora de video?  
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANUlEQVR4nO3OMQ2AABAAsSPBCj7fFwtCmJHAjAU2QtIq6DIzW7UHAMBfnGt1V8fHEQAA3rsexOkF3va0dq8AAAAASUVORK5CYII=)  
**P5. Prioridades**  
**Qué demuestra:** que un caso urgente se adelanta a los que ya estaban esperando.  
**Cómo:**  
1. Mandar varios casos para que se forme fila:  
2. python cliente/cliente.py carga ./dataset_real --filtro audio --concurrentes 4  
   
3. Mientras las colas del dashboard muestran tareas esperando, mandar uno urgente:  
4. python cliente/cliente.py enviar ./dataset_real/caso_012_video --prioridad 10  
   
**Qué capturar:** la línea de tiempo, donde las tareas del caso urgente empiezan antes que las de casos enviados antes.  
| | | | | |  
|-|-|-|-|-|  
| **Caso** | **Prioridad** | **Hora de envío** | **Hora en que empezó su primera tarea** | **Hora de fin** |   
|   |   |   |   |   |   
   
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANUlEQVR4nO3OQQmAABRAsSd4EKxgBjP+Asa0hxW8ibAl2DIzR3UFAMBf3Gu1VefXEwAAXtsfSqwDVbgKngwAAAAASUVORK5CYII=)  
**P6. Se cae una computadora**  
**Qué demuestra:** que si un worker se apaga a mitad de una tarea, otro la termina.  
**Cómo:**  
1. Mandar un caso con videos pesados (por ejemplo caso_036_heterogeneo_extremo).  
2. Cuando un worker esté convirtiendo un video (se ve el % en el detalle del caso), **cortarlo de golpe**: desconectar el Wi-Fi, apagar la PC o docker kill worker-2.  
3. Mirar que esa tarea pase a otro worker (en el detalle aparece "redistribuida") y que el worker caído aparezca **desconectado** a los 15 s.  
| | | | | | |  
|-|-|-|-|-|-|  
| **Tarea** | **Worker que se cayó** | **% al caer** | **Worker que la retomó** | **Cuánto tardó en retomarse** | **Estado final del caso** |   
|   |   |   |   |   |   |   
   
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANklEQVR4nO3OUQmAABBAsSeYxZyXSzCJASxgACv4J8KWYMvMbNURAAB/ca7VXe1fTwAAeO16AKe+BdmJqrPdAAAAAElFTkSuQmCC)  
**P7. Una computadora se satura**  
**Qué demuestra:** que una computadora con la CPU al límite deja de recibir trabajo y los otros lo absorben.  
**Cómo:**  
1. En una de las computadoras, generar carga de CPU con otro programa (por ejemplo, stress --cpu 8, o abrir algo pesado).  
2. Mandar casos al sistema.  
3. Mirar que ese worker pase a **saturado**, que deje de tomar tareas y que vuelva a  **libre** al cerrar el otro programa.  
| | | | |  
|-|-|-|-|  
| **Momento** | **CPU de esa computadora** | **Estado del worker** | **Tareas que tomó en ese rato** |   
| Antes |   |   |   |   
| Con la carga externa |   |   |   |   
| Después |   |   |   |   
   
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANElEQVR4nO3OQQmAUBBAwSf8GGLWDWFDY3ixgjcRZhLMNjNHdQYAwF9cq1rV/vUEAIDX7gcRXAQ2s/16gwAAAABJRU5ErkJggg==)  
**P8. Errores y cierre del caso (barrier/join)**  
**Qué demuestra:** que el caso se cierra recién cuando terminaron  **todas** sus tareas, y que los fallos quedan explicados en el reporte.  
**Cómo:** armar un caso con archivos válidos y uno que no se puede procesar:  
mkdir -p pruebas/caso_con_fallos  
 cp dataset_real/caso_001_audio_conversion/* pruebas/caso_con_fallos/  
 echo "texto" > pruebas/caso_con_fallos/notas.txt  
 head -c 50000 /dev/urandom > pruebas/caso_con_fallos/video_danado.mp4  
 python cliente/cliente.py enviar pruebas/caso_con_fallos  
   
Para probar los **reintentos**, durante una conversión cortar unos segundos la red de un worker: la tarea pasa a  **reintentando** y después se recupera.  
**Qué capturar:** el estado  **parcialmente completado**, el resumen del reporte con el motivo de cada fallo, y que el caso no se cerró hasta que terminó la última tarea.  
| | | | | | | |  
|-|-|-|-|-|-|-|  
| **Caso** | **Tareas** | **Completadas** | **Fallidas (por qué)** | **Reintentos** | **Estado final** | **¿Se cerró al final?** |   
|   |   |   |   |   |   |   |   
   
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANklEQVR4nO3OMQ2AABAAsSPBCj7fFRYQwYwEZiywEZJWQZeZ2ao9AAD+4lyruzq+ngAA8Nr1AMTJBeJDClAyAAAAAElFTkSuQmCC)  
**P9. Cancelar un caso**  
**Qué demuestra:** que al cancelar, lo que falta ya no se procesa.  
**Cómo:**  
python cliente/cliente.py enviar ./dataset_real/caso_012_video  
 python cliente/cliente.py cancelar case-xxxxxxxx      # mientras se está procesando  
   
| | | | |  
|-|-|-|-|  
| **Tareas totales** | **Terminadas antes de cancelar** | **Canceladas** | **Estado final** |   
|   |   |   | cancelado |   
   
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANElEQVR4nO3OQQmAABRAsad4EEtY9QcxnUms4E2ELcGWmTmrKwAA/uLeqrU6vp4AAPDa/gDzXgM37EF77AAAAABJRU5ErkJggg==)  
**P10. Casos armados automáticamente**  
**Qué demuestra:** que el sistema puede agrupar archivos en casos por sí solo, según sus metadatos.  
**Cómo:**  
python cliente/cliente.py auto ./dataset_real --por evento --concurrentes 3 --metricas pruebas/p10_por_evento.csv  
   
**Qué capturar:** la lista de casos creados (uno por evento) y el reporte de uno de ellos, con los metadatos de cada archivo.  
| | | | |  
|-|-|-|-|  
| **Agrupado por** | **Casos creados** | **Archivos** | **Tiempo total (s)** |   
| evento |   |   |   |   
   
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAANklEQVR4nO3OMQ2AABAAsSNBACPq8MH2NpGACyywEZJWQZeZ2aszAAD+4l6rrTq+ngAA8Nr1AL/KBEe6dElaAAAAAElFTkSuQmCC)  
**P11. Archivos pesados (400–600 MB)**  
**Qué demuestra:** el escenario que pidió el profesor. Archivos enormes no traban el sistema, y los demás casos siguen avanzando mientras tanto.  
**Cómo:**  
# 1. el caso de 6 archivos de ~600 MB  
 python cliente/cliente.py enviar ./dataset_real/caso_036_heterogeneo_extremo  
 # 2. mientras se procesa, mandar casos livianos  
 python cliente/cliente.py carga ./dataset_real --filtro imagenes --concurrentes 3  
   
**Qué capturar:** cuánto tardó la subida, el % de avance de los videos grandes, la línea de tiempo (las tareas chicas siguen corriendo en paralelo) y la CPU y RAM del worker que procesa el archivo grande.  
| | | | | | | |  
|-|-|-|-|-|-|-|  
| **Archivo** | **Tamaño** | **Subida (s)** | **Bajada al worker (s)** | **Conversión (min)** | **Worker** | **Tamaño del resultado** |   
|   |   |   |   |   |   |   |   
   
**Para comentar:** con archivos grandes, ¿qué fue lo más lento: la red, el disco o la CPU? ¿Se trabaron los otros casos?  
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnEAAAACCAYAAAA3pIp+AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAAM0lEQVR4nO3KsQ0AIRAEsUW6Qij1KvnevhMSYmKQ7GiCGd09k3wBAOAVf+2o4wYAwE1qAdYuAy151mgcAAAAAElFTkSuQmCC)  
**Conclusiones**  
Escribir en pocas líneas:  
- qué tan bien escaló el sistema al sumar computadoras;  
- qué fue lo más lento (red, coordinador, tareas pesadas);  
- qué mostró la comparación entre workers genéricos y especializados;  
- cómo se comportó ante caídas y errores;  
- qué mejorarían.  
