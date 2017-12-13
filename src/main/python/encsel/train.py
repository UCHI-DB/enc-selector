import sys
import tensorflow as tf
from tensorflow.contrib.learn.python.learn.datasets.base import load_csv_with_header

num_feature = 21;
hidden_dim = 1000;


def build_graph(num_class):
    x = tf.placeholder(tf.float32, shape=[None, num_feature], name="x")
    y = tf.placeholder(tf.float32, shape=[None, num_class], name="y")
    with tf.name_scope("layer1"):
        w1 = tf.Variable(tf.truncated_normal([num_feature,hidden_dim],stddev=0.1), name="w1")
        b1 = tf.Variable(tf.zeros([hidden_dim]), name="b1")
        layer1 = tf.tanh(tf.matmul(x, w1) + b1,name="tanh")
    with tf.name_scope("layer2"):
        w2 = tf.Variable(tf.truncated_normal([hidden_dim, num_class],stddev=0.1), name="w2")
        b2 = tf.Variable(tf.zeros([num_class]), name="b2")
        layer2 = tf.matmul(layer1, w2) + b2

    loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels=y, logits=layer2),name="loss")

    return loss


def train(data_file, model_path, num_class):
    dataset = load_csv_with_header(data_file, tf.float32, tf.float32, 22);
    with tf.Session() as sess:
        build_graph(num_class);

        sess.run(tf.global_variables_initializer());

def print_usage():
    print("Usage: train.py <data_file> <model_path> <num_class>")
    exit(0)


def main():
    if len(sys.argv) != 4:
        print_usage();
    data_file = sys.argv[1];
    model_path = sys.argv[2];
    num_class = int(sys.argv[3]);
    train(data_file, model_path, num_class);


if __name__ == "__main__":

    main()
