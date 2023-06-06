# To run the example, start two terminals:
# python simple_psi.py -rank 0 -protocol KKRT_PSI_2PC -in_path ./psi_1.csv -field_names id -out_path ./p1.out --precheck_input false
# python simple_psi.py -rank 1 -protocol KKRT_PSI_2PC -in_path ./psi_2.csv -field_names id -out_path ./p2.out --precheck_input false

from absl import app, flags

import spu.binding.psi as psi
import spu.binding._lib.link as link

flags.DEFINE_string("protocol", "ECDH_PSI_2PC", "psi protocol, see `spu/psi/psi.proto`")
flags.DEFINE_integer("rank", 0, "rank: 0/1/2...")
flags.DEFINE_string("party_ips", "127.0.0.1:9307,127.0.0.1:9308", "party addresses")
flags.DEFINE_string("in_path", "data.csv", "data input path")
flags.DEFINE_string("field_names", "id", "csv file filed name")
flags.DEFINE_string("out_path", "simple_psi_out.csv", "data output path")
flags.DEFINE_integer("receiver_rank", -1, "main party for psi, will get result")
flags.DEFINE_bool("output_sort", True, "whether to sort result")
flags.DEFINE_bool("precheck_input", True, "whether to precheck input dataset")
flags.DEFINE_integer("bucket_size", 1048576, "hash bucket size")
flags.DEFINE_bool("ic_mode", False, "whether to run in interconnection mode")
FLAGS = flags.FLAGS


def setup_link(rank):
    lctx_desc = link.Desc()
    lctx_desc.id = f"root"

    lctx_desc.recv_timeout_ms = 2 * 60 * 1000
    lctx_desc.connect_retry_times = 180
    if FLAGS.ic_mode:
        lctx_desc.brpc_channel_protocol = "h2:grpc"

    ips = FLAGS.party_ips.split(",")
    for i, ip in enumerate(ips):
        lctx_desc.add_party(f"id_{i}", ip)
        print(f"id_{i} = {ip}")

    return link.create_brpc(lctx_desc, rank)


def main(_):
    selected_fields = FLAGS.field_names.split(",")

    config = psi.BucketPsiConfig(
        psi_type=psi.PsiType.Value(FLAGS.protocol),
        broadcast_result=True if FLAGS.receiver_rank < 0 else False,
        receiver_rank=FLAGS.receiver_rank if FLAGS.receiver_rank >= 0 else 0,
        input_params=psi.InputParams(
            path=FLAGS.in_path,
            select_fields=selected_fields,
            precheck=FLAGS.precheck_input,
        ),
        output_params=psi.OuputParams(path=FLAGS.out_path, need_sort=FLAGS.output_sort),
        bucket_size=FLAGS.bucket_size,
        curve_type=psi.CurveType.CURVE_25519,
    )
    report = psi.bucket_psi(setup_link(FLAGS.rank), config, FLAGS.ic_mode)
    print(
        f"original_count: {report.original_count}, intersection_count: {report.intersection_count}"
    )


if __name__ == '__main__':
    app.run(main)
