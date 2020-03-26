"""
In this collaborative filtering model, the matrix with shape (m, n) will be regarded as a list of m vectors with size n.
The vector is the representation of a user or item, and the matrix is the set of the users or items.

To save memory, we use np.int16 and np.float16 as data type of arrays.
"""

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse import csr
from tqdm import tqdm


def dtype_cast(arr: np.ndarray or csr.csr_matrix):
    dtype = str(np.dtype(arr.dtype))
    if dtype.startswith('int'):
        if arr.dtype != np.int16:
            arr = arr.astype(np.int16)
    elif dtype.startswith('float'):
        if arr.dtype != np.float16:
            arr = arr.astype(np.float16)
    else:
        raise TypeError('%s not supported' % arr.dtype)
    return arr


# ==================================================================================================================== #
# Numpy matrix part
def cosine_distance(vec1: np.ndarray, vec2: np.ndarray):
    """
    Cosine distance of two vectors
    :param vec1: vector1
    :param vec2: vector2
    :return: cosine distance
    """
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1, ord=2) * np.linalg.norm(vec2, ord=2))


def vec_matrix_dot(vec: np.ndarray, mat: np.ndarray):
    """
    Calculate the dot production of a vector and a matrix
    :param vec: vector, 1D array with shape (n,)
    :param mat: matrix, 2D array with shape (m, n)
    :return: dot production result
    """
    return np.dot(np.array([vec]), mat.T)


def matrix_l2_norm(mat: np.ndarray):
    """
    Calculate the square of each vector in the matrix
    :param mat: matrix, 2D array
    :return:a vector of l2 norm of each vector in the matrix
    """
    return np.linalg.norm(mat, 2, 1)


def vec_mat_cosine(vec: np.ndarray, mat: np.ndarray, cast=True):
    """
    Calculate the cosine distance of the vector and each vector in the matrix
    :param cast: whether to cast dtype to 16bit
    :param vec: vector, 1D array with shape (n,)
    :param mat: matrix, 2D array with shape (m, n)
    :return: cosine distance of the vector and each vector in the matrix
    """
    if cast:
        vec = dtype_cast(vec)
        mat = dtype_cast(mat)
    vec_l2 = np.linalg.norm(vec, ord=2).astype(np.float16)
    mat_l2 = matrix_l2_norm(mat).astype(np.float16)
    return (vec_matrix_dot(vec, mat) / (vec_l2 * mat_l2))[0]


def mat_cosine(mat: np.ndarray, cast=True):
    """
    Matrix as lines of vectors, calculate the cosine distance of every two vectors
    :param cast: whether to cast dtype to 16bit
    :param mat: matrix
    :return: matrix, index i,j is the cosine distance of the i_th vector and j_th vector
    """
    if cast:
        mat = dtype_cast(mat)
    mat_mat_dot = np.matmul(mat, mat.T)
    mat_l2 = np.expand_dims(matrix_l2_norm(mat), axis=-1).astype(np.float16)
    mat_l2 = np.matmul(mat_l2, mat_l2.T)
    return mat_mat_dot/mat_l2
# ==================================================================================================================== #


# ==================================================================================================================== #
# Compressed matrix(dict) part
def compressed_vec_cosine(vec1: dict, vec2: dict):
    """
    Cosine distance of two dict of vectors
    :param vec1: vector1
    :param vec2: vector2
    :return: cosine distance
    """
    elements = {**vec1, **vec2}.keys()
    vec_size = len(elements)
    elements = dict(enumerate(elements))
    vec_arr1, vec_arr2 = np.zeros(vec_size), np.zeros(vec_size)
    for index, element in elements.items():
        vec_arr1[index] = vec1.get(element, 0)
        vec_arr2[index] = vec2.get(element, 0)
    return cosine_distance(vec_arr1, vec_arr2)


def compressed_mat_cosine(vec: dict, mat: dict):
    """
    Calculate the cosine distance of the vector and each vector in the matrix
    :param vec: vector
    :param mat: matrix
    :return: dict of cosine distance, {key: cosine distance}
    """
    ret = dict()
    for key, dic in mat.items():
        ret[key] = compressed_vec_cosine(vec, dic)
    return ret
# ==================================================================================================================== #


# ==================================================================================================================== #
# Sparse matrix part
def sparse_vec_mat_dot(vec: csr.csr_matrix, mat: csr.csr_matrix):
    """
    Calculate the dot production of sparse vector and matrix
    :param vec: sparse vector
    :param mat: sparse matrix
    :return: dot production result, the result is an array
    """
    return vec.dot(mat.T).toarray()[0]


def sparse_matrix_l2_norm(mat: csr.csr_matrix):
    """
    Calculate the l2 norm of the sparse matrix
    :param mat: matrix
    :return: l2 norm value of each vector in the matrix, the result is an array
    """
    dot_prod = np.array(mat.power(2).sum(axis=1), dtype=np.float16)
    return np.hstack(np.sqrt(dot_prod))


def sparse_vec_mat_cosine(vec: csr.csr_matrix, mat: csr.csr_matrix, cast=True):
    """
    Calculate the cosine distance of the vector and each vector in the matrix
    :param cast: whether to cast dtype to 16bit
    :param vec: vector, sparse array
    :param mat: matrix, sparse array
    :return: cosine distance of the vector and each vector in the matrix
    """
    if cast:
        vec = dtype_cast(vec)
        mat = dtype_cast(mat)
    vec_l2 = sparse_matrix_l2_norm(vec)
    return sparse_vec_mat_dot(vec, mat) / (vec_l2 * sparse_matrix_l2_norm(mat))


def sparse_iter_mat_cosine(mat: csr_matrix, top_k=400, cast=True):
    """
    For each vector in the matrix, find the most similar top_k vectors.
    :param cast: whether to cast dtype to 16bit
    :param mat: matrix
    :param top_k: topK
    :return: an array with shape (num_vec, top_k), each row is the index of the top_k vectors
    """
    num_vec = mat.shape[0]
    if cast:
        mat = dtype_cast(mat)
    mat_l2 = sparse_matrix_l2_norm(mat)
    ret = np.empty((num_vec, top_k), dtype=np.int32)
    for i in tqdm(range(num_vec)):
        vec = mat[i]
        vec_l2 = sparse_matrix_l2_norm(vec)
        cosine_vec = sparse_vec_mat_dot(vec, mat) / (vec_l2 * mat_l2)
        ret[i] = np.argsort(cosine_vec)[:-top_k-1:-1]
    return ret


def sparse_mat_cosine(mat: csr.csr_matrix, cast=True):
    """
    Matrix as lines of vectors, calculate the cosine distance of every two vectors
    :param cast: whether to cast dtype to 16bit
    :param mat: matrix
    :return: matrix, index i,j is the cosine distance of the i_th vector and j_th vector
    """
    if cast:
        mat = dtype_cast(mat)
    dot_prod = mat.dot(mat.T)
    mat_l2 = np.sqrt(mat.power(2).sum(axis=1)).astype(np.float16)
    mat_l2 = np.dot(mat_l2, mat_l2.T)
    return np.array(dot_prod / mat_l2)

# ==================================================================================================================== #


def test():
    sp_v1 = np.array([1, 2, 3, 5, 4])
    sp_v2 = np.array([1, 2, 3, 4, 5])
    sp_v3 = np.array([1, 3, 2, 4, 4])
    sp_mat = np.array([sp_v1, sp_v2, sp_v3])

    # print(sp_mat.shape)
    # sp_cosine = cosine_distance(sp_v1, sp_v2)
    # print(sp_cosine)

    sp_mat_dot = vec_matrix_dot(sp_v1, sp_mat)
    # print(sp_mat_dot)

    sp_mat_square = matrix_l2_norm(sp_mat)
    # print(sp_mat_square)

    sp_mat_cosine = vec_mat_cosine(sp_v1, sp_mat)
    # print(sp_mat_cosine.dtype)

    sp_mat_mat_cosine = mat_cosine(sp_mat)
    # print(sp_mat_mat_cosine)
    # print(sp_mat_mat_cosine.dtype)

    # sp_dict_vec1 = {1: 1, 2: 2, 3: 3, 4: 5, 5: 4}
    # sp_dict_vec2 = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
    #
    # sp_dic_mat = {1: sp_dict_vec1, 2: sp_dict_vec2}
    #
    # # print(compressed_vec_cosine(sp_dict_vec1, sp_dict_vec2))
    # print(compressed_mat_cosine(sp_dict_vec1, sp_dic_mat))

    sp_sparse_vec1 = csr_matrix(sp_v1)
    sp_sparse_vec2 = csr_matrix(sp_v1)
    sp_sparse_vec3 = csr_matrix(sp_v3)
    sp_sparse_mat = csr_matrix(sp_mat)

    # sp_sparse_mat_dot = sparse_matrix_dot(sp_sparse_vec1, sp_sparse_mat)
    # print(sp_sparse_mat_dot)
    #
    # sp_sparse_mat_l2 = sparse_matrix_l2_norm(sp_sparse_mat)
    # print(sp_sparse_mat_l2)
    #
    sp_sparse_cos_ret = sparse_vec_mat_cosine(sp_sparse_vec1, sp_sparse_mat)
    # print(sp_sparse_cos_ret)

    sp_sparse_mat_cosine = sparse_mat_cosine(sp_sparse_mat)
    # print(sp_sparse_mat_cosine)
    # print(sp_sparse_mat_cosine.dtype)
    # print(sp_sparse_mat_cosine[2][0])
    # print(type(sp_sparse_mat_cosine[2][0]))
    # print(type(sp_sparse_mat_cosine))


def load_test():
    import pandas as pd
    from sephora_cn_recommendation_engine.lib.models.matrix import NumpyMatrix, SparseMatrix

    # From csv or npy file
    sp_df = pd.read_csv("")
    npy_mat = NumpyMatrix(sp_df, target_key='qty').calculate_matrix()
    sparse_mat = SparseMatrix(sp_df, target_key='qty').calculate_sparse_matrix()


if __name__ == '__main__':
    # test()

    load_test()

