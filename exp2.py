"""
Matrix Multiplication using MapReduce
"""

import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

class MatrixMultiplication(MRJob):

    def configure_args(self):
        super(MatrixMultiplication, self).configure_args()
        self.add_passthru_arg('--rowsA', type=int, help='Number of rows in matrix A')
        self.add_passthru_arg('--colsA', type=int, help='Number of columns in matrix A')
        self.add_passthru_arg('--colsB', type=int, help='Number of columns in matrix B')


    def mapper_init(self):
        self.rowsA = self.options.rowsA
        self.colsA = self.options.colsA
        self.colsB = self.options.colsB


    def mapper(self, _ , line):
        """mapper code"""

        matrix, row, col, value = line.strip().split()
        if matrix == "matrixA":

            for j in range(self.colsB):
                yield (int(row), j), ("matrixA", col, value)

        if matrix == "matrixB":

            for i in range(self.rowsA):
                yield (i, int(col)), ("matrixB", row, value)
        


    def reducer(self, key, values):
        

        matrixA_values = {}

        matrixB_values = {}

        for value in values:

            if value[0] == "matrixA":
                matrixA_values[value[1]] = value[2]

            else:
                matrixB_values[value[1]] = value[2]

        total = 0
        for k in matrixA_values.keys():
            if k in matrixB_values:
                total += int(matrixA_values[k]) * int(matrixB_values[k])

        yield key, total
    



if __name__ == "__main__":
    MatrixMultiplication().run()