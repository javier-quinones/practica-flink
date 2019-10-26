package mx.cic

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import java.io._
import java.util.Properties
/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCountArchivos {

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]) {

    val archivoSalida = new File("/home/javier/Documents/CIC/cic-flink/src/main/output/salida.txt")
    val bufferedWriter = new BufferedWriter(new FileWriter(archivoSalida))

    var strArchivosEncontrados = ""   // cadena que almacena el nombre de todos los archivos, e.g. file1, file2, file3
    var numeroDeArchivos = 0          // contador, almacena el numero total de archivos que se encontraron
    var strTotalesPorArchivo = ""     // cadena, e.g.    #totalWordsOfFile1, #totalWordsOfFile2, #totalWordsOfFile3
    var tmpNumPalabrasEnArchivoActual = 0   // variable temporal, almacena el numero de palabras en el archivo actual
    var totalPalabrasAcrossArchivos = 0  // Acumulador; suma el numero de palabras de todos los archivos
    var coleccionPalabraConteo = ""      // Cadena que contiene (llave, valor), donde llave es palabra y valor, conteos.

    val listaDeArchivos = getListFiles(args(0))   // Primero, obtener la lista de archivos exitentes en el directorio
                                                  // recibido como primer parametro.

    for (archivoActual <-listaDeArchivos) {       // para cada archivo encontrado,
      numeroDeArchivos+=1                               // incrementar el contador de numero de archivos

                                                        // contar el numero de palabras en este archivo
      tmpNumPalabrasEnArchivoActual = numeroPalabrasEnArchivo(archivoActual.toString())

        // conforme se vayan encontrando archivos, coleccionar los nombres (en la cadena strArchivosEncontrados)
        // y tambien una cadena con los totales de palabras por archivo, e.g. "12,13,14" (variable strTotalesPorArchivo)
      if (strArchivosEncontrados == "") {                       // la primera parte de la cadena no debera llevar comas
        strArchivosEncontrados = archivoActual.toString()
        strTotalesPorArchivo = tmpNumPalabrasEnArchivoActual.toString()
      } else {
        strArchivosEncontrados = strArchivosEncontrados + ", " + archivoActual
        strTotalesPorArchivo = strTotalesPorArchivo + ", " + tmpNumPalabrasEnArchivoActual.toString()
      }

      totalPalabrasAcrossArchivos += tmpNumPalabrasEnArchivoActual   // actualizar el acumulador de total de palabras
                                                                     // across archivos
                                                                     // Y tambien almacenar el par (palabra, conteo),
                                                                     // de las palabras encontradas en el archivo actual
      coleccionPalabraConteo += archivoActual.toString() + "\n" + palabrasConConteosEnArchivo(archivoActual.toString()) + "\n"

    }

    // ****************************
    // comienza escritura de salida
    // ****************************

      //file1, file2, file3, #Files(3)
    bufferedWriter.write(strArchivosEncontrados + ", " + numeroDeArchivos)
    bufferedWriter.write("\n" + "*************"+"\n")

      //#totalWordsOfFile1, #totalWordsOfFile2, #totalWordsOfFile3
    bufferedWriter.write(strTotalesPorArchivo)
    bufferedWriter.write("\n" + "*************" + "\n")

      //#TotalWordsRead(totalWordsOfFile1+totalWordsOfFile2+totalWordsOfFile3)
    bufferedWriter.write(totalPalabrasAcrossArchivos.toString())
    bufferedWriter.write("\n" + "*************" + "\n")

      //nameFile
      //word, value
    bufferedWriter.write(coleccionPalabraConteo)

    bufferedWriter.close()        // cerrar la conexion de escritura al archivo

  }

      // devuelve el par (palabra, conteo), para cada palabra encontrada en el archivo recibido como parametro
  def palabrasConConteosEnArchivo(path:String): String = {

    val localLines = env.readTextFile(path)   // read text file from local files system

    val counts = localLines.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    return counts.collect().toString()
  }

      // devuelve un entero, indicando el numero de palabras unicas encontradas en el archivo
  def numeroPalabrasEnArchivo(path:String): Int = {

    val localLines = env.readTextFile(path)       // read text file from local files system

    val counts = localLines.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .count()            // en lugar de usar .sum(1), .count() permite tener la propiedad .intValue despues

    return counts.intValue
  }

      // devuelve una lista, con apuntadores a los archivos existentes en el directorio recibido como parametro
  def getListFiles(directory: String): List[File] = {
    val d = new File(directory)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    }
    else List[File]()
  }

}