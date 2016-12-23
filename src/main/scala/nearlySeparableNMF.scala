/* Computes a NMF factorization of a nearly-separable matrix, using the 
algorithm of 
``Scalable methods for nonnegative matrix factorizations of near-separable
tall-and-skinny matrices. Benson et al.''

*/
package org.apache.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.{DenseMatrix}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow}
import edu.berkeley.cs.amplab.mlmatrix.{RowPartitionedMatrix, RowPartition, modifiedTSQR} 
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, diag}

import org.apache.spark.mllib.optimization.NNLS
import scala.math

import org.nersc.io._
import java.io.File
import java.util.Calendar
import java.text.SimpleDateFormat


object nmf {
    def report(message: String, verbose: Boolean = true) = {
        println("run report")
        val now = Calendar.getInstance().getTime()
        val formatter = new SimpleDateFormat("H:m:s")
        if (verbose) {
            println("STATUS REPORT (" + formatter.format(now) + "): " + message)
        }
    }

    case class NMFDecomposition(colbasis : DenseMatrix, loadings : DenseMatrix) {
        def H : DenseMatrix = colbasis
        def W : DenseMatrix = loadings
    }

    def fromBreeze(mat: BDM[Double]): DenseMatrix = {
        println("run fromBreeze")
        new DenseMatrix(mat.rows, mat.cols, mat.data, mat.isTranspose)
    }

    def main(args: Array[String]) = {
        println("run main")
        val conf = new SparkConf().setAppName("nearSeparableNMF")
        val sc = new SparkContext(conf)
        sys.addShutdownHook( { sc.stop() } )
        appMain(sc, args)
//        println(A)
  //      println("----------------------------------")
    //    println(W)
      //  println("----------------------------------")
       // println(finalH)
        sc.stop()
    }

   def fullNMF(A : RowPartitionedMatrix, rank : Int) : Tuple3[Array[Int], BDM[Double], BDM[Double]] = {
        println("run fullNMF")
        val (colnorms, rmat) = new modifiedTSQR().qrR(A)
        val normalizedrmat = rmat * diag( BDV.ones[Double](rmat.cols) :/ colnorms)
        val (extremalcolindices, finalH) = xray.computeXray(normalizedrmat, rank)
        val W = A.mapPartitions( mat => { 
            val newMat = BDM.zeros[Double](mat.rows, extremalcolindices.length)
            (0 until extremalcolindices.length).foreach { 
                colidx => newMat(::, colidx) := mat(::, extremalcolindices(colidx)) }
            newMat
          }).collect()
	(extremalcolindices, W, finalH * diag( BDV.ones[Double](rmat.cols) :* colnorms))
    }

    def appMain(sc: SparkContext, args: Array[String]) = {
        println("run appMain")
        val inpath = args(0)
        val variable = args(1)
        val numrows = args(2).toLong
        val numcols = args(3).toInt
        val partitions = args(4).toLong
        val rank = args(5).toInt
        val outdest1 = args(6)
 	val outdest2 = args(7)

//        val A = loadH5Input(sc, inpath, variable, numrows, numcols, partitions)
        val A = loadcsv(sc, inpath, variable, numrows, numcols, partitions)
	/*File tmpfile= new File(inpath)
	val tmpmatrix = breeze.linglg.csvread(tmpfile)
	val matrixrdd = sc.parallelize(1 to 1,1).mapPartitions {part =>
		val mat = tmpmatrix
		Iterator(mat)
	}*/
	//val A = RowPartitionedMatrix.fromMatrix(matrixrdd)
        //val A=RowPartitionedMatrix.createRandom(sc,50,10,5,true)
        println("-----A------")
        println(A)
        println("-----A-------")
        /* don't cache or count, since we don't need to return the true columns from the matrix */
//        A.rdd.cache() // store deserialized in memory 
//        A.rdd.count() // don't explicitly materialize, since we want to do pseudo one-pass, so let TSQR pay price of loading data

        // note this R is not reproducible between runs because it depends on the row partitioning
        // and the tree reduction order
        report("calling TSQR")
        val (colnorms, rmat) = new modifiedTSQR().qrR(A)
        report("TSQR worked")
        report("normalizing the columns of R")
        val normalizedrmat = rmat * diag( BDV.ones[Double](rmat.cols) :/ colnorms)
        report("done normalizing")
        report("starting xray")
        val (extremalcolindices, finalH) = xray.computeXray(normalizedrmat, rank)
        report("ending xray")

        /* THIS IS TOO LARGE (87GB, and too many indices) to collect for 1.6TB dayabay dataset 
        // Make a pass back over the data to extract the columns we care about
        // apparently RowPartitionedMatrix.collect is quite inefficient*/
              

              
       val W = A.mapPartitions( mat => { 
            val newMat = BDM.zeros[Double](mat.rows, extremalcolindices.length)
            (0 until extremalcolindices.length).foreach { 
                colidx => newMat(::, colidx) := mat(::, extremalcolindices(colidx)) }
            newMat
          }).collect()
 	println("---------------------------------------------")
        println(W)
        println("---------------------------------------------")
        println(finalH)
        println("---------------------------------------------")
        println(W*finalH)
	println("---------------------------------------------")

	val tempwritefilew = new File(outdest1)
	val resultw = breeze.linalg.csvwrite(tempwritefilew,W)

	val tempwritefileh = new File(outdest2)
	val resulth = breeze.linalg.csvwrite(tempwritefileh,finalH)
          

        // TODO: check correctness
        // one way to check rmat is correct is to right multiply A by its inverse and check for orthogonality
        // check the colnorms by explicitly constructing them

        // TODO: return/write out decomposition

    }

    /* returns argmin || R - R[::, colindices]*H ||_F s.t. H >= 0 */
    def computeH(R : BDM[Double], colindices: Array[Int]) : BDM[Double] = {
        println("run computeH")
        val RK = BDM.horzcat( (0 until colindices.length).toArray.map( colidx => R(::, colindices(colidx)).asDenseMatrix.t ):_* )

        val H = BDM.zeros[Double](RK.cols, R.cols)
        val ws = NNLS.createWorkspace(RK.cols)
        val ata = (RK.t*RK).toArray
       
        for(colidx <- 0 until R.cols) {
            val atb = (RK.t*R(::, colidx)).toArray
            val h = NNLS.solve(ata, atb, ws)
            H(::, colidx) := BDV(h)
        }

        H
    }

    // note numrows, numcols are currently ignored, and the dimensions are taken from the variable itself
    def loadH5Input(sc: SparkContext, inpath: String, variable: String, numrows: Long, numcols: Int, repartition: Long) : RowPartitionedMatrix = {
        println("run loadH5Input")
        val temprows = read.h5read_irow(sc, inpath, variable, repartition)
        println("-----------------")
        println(temprows)
        println("-----------------")
        println(temprows.first)
        println("---------------")

        def buildRowBlock(iter: Iterator[IndexedRow]) : Iterator[RowPartition] = {
            val mat = BDM.zeros[Double](iter.length, numcols)
            for(rowidx <- 0 until iter.length) {
                val currow = iter.next.vector.toBreeze.asInstanceOf[BDV[Double]]
                mat(rowidx, ::) := currow.map(x => if (x < 0) 0 else math.log(x + 1)).t
            }
            Array(RowPartition(mat)).toIterator
        }

        new RowPartitionedMatrix(temprows.mapPartitions(buildRowBlock))
    }
    
   def loadcsv(sc: SparkContext, inpath: String, variable: String, numrows: Long, numcols: Int, repartition: Long) : RowPartitionedMatrix = {
        val tmpfile = new File(inpath)
        val tmpmatrix = breeze.linalg.csvread(tmpfile)
        val rowsPerPart = numrows.toInt/repartition
        val matrixParts = sc.parallelize(1 to repartition.toInt, repartition.toInt).mapPartitionsWithIndex { (part,iter) =>
        if(part!=repartition.toInt){
           val beginrow = (part-1)*rowsPerPart
           val endrow = beginrow + rowsPerPart - 1
           val tmppartmatrix = tmpmatrix(beginrow.toInt to endrow.toInt,::)
           Iterator(tmppartmatrix)
        }else{
            val beginrow = (part-1)*rowsPerPart
            val endrow = tmpmatrix.rows - 1
            val tmppartmatrix = tmpmatrix(beginrow.toInt to endrow.toInt,::)
            Iterator(tmppartmatrix)
            } 
       } 
       val B=RowPartitionedMatrix.fromMatrix(matrixParts)  
       B 
   }
}

