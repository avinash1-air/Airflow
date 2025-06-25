import os

import sys

import time

import math

import click

import socket

import signal

import itertools

import traceback

import pandas as pd

import numpy as np

from functools import reduce

from dotenv import load_dotenv

from datetime import datetime

from datetime import date, timedelta

from sqlalchemy.orm.exc import NoResultFound

 

from returns.src import *

from returns.src.corelogger import *

from returns.src.datamodels import cRefFncDataAttrStaging1

from returns.src.datamodels import c_ref_fnc_identity_gross

from returns.src.datamodels import c_staging1

 

load_dotenv()

log = CoreLogger()

 

class RequestToTerminateException(Exception):

    pass

 

def signal_term_handler(signal, frame):

    log.info("Shut down hook activated")

    raise RequestToTerminateException()

 

#while os.path.exists("./grossreturns.pid"):

#    log.info("program is running, will wait 300 seconds...")

#    time.sleep(420)

 

# - register shut down hook

open("./grossreturns.pid", 'w').write(str(os.getpid()))

signal.signal(signal.SIGTERM, signal_term_handler)

 

lst_nobenchmark = [

   'JIS_CSAF',

   'CITFUND',

   'GCFUND',

   'PRI_CIT',

   'PRI_MUT',

   'HARBOR',

   'GALAXY',

   'FARFUND',

   'DBSEL',

   'ABCAP',

   'CCNR'

]

 

dict_inceptiondt = {}

dict_inceptiondt[('APFUND','OYGLSNE')]    = './sql/inception_date_apfund.sql'

dict_inceptiondt[('JIS_CSAF','FUND')]      = './sql/inception_date_isg.sql'

dict_inceptiondt[('JIS_CSAF','CMDTY')]     = './sql/inception_date_isg.sql'

dict_inceptiondt[('JIS_CSAF','EQTY')]      = './sql/inception_date_isg.sql'

dict_inceptiondt[('JIS_CSAF','FIXINC')]    = './sql/inception_date_isg.sql'

dict_inceptiondt[('JIS_CSAF','CMDTYAGG')]  = './sql/inception_date_isg.sql'

dict_inceptiondt[('JIS_CSAF','EQTYAGG')]   = './sql/inception_date_isg.sql'

dict_inceptiondt[('JIS_CSAF','FIXINCAGG')] = './sql/inception_date_isg.sql'

dict_inceptiondt[('CITFUND','FUND')]       = './sql/inception_date_isg.sql'

dict_inceptiondt[('CITFUND','CMDTY')]      = './sql/inception_date_isg.sql'

dict_inceptiondt[('CITFUND','EQTY')]       = './sql/inception_date_isg.sql'

dict_inceptiondt[('CITFUND','FIXINC')]     = './sql/inception_date_isg.sql'

dict_inceptiondt[('CITFUND','CMDTYAGG')]   = './sql/inception_date_isg.sql'

dict_inceptiondt[('CITFUND','EQTYAGG')]    = './sql/inception_date_isg.sql'

dict_inceptiondt[('CITFUND','FIXINCAGG')]  = './sql/inception_date_isg.sql'

dict_inceptiondt[('GCFUND','FUND')]        = './sql/inception_date_isg.sql'

dict_inceptiondt[('GCFUND','CMDTY')]       = './sql/inception_date_isg.sql'

dict_inceptiondt[('GCFUND','FIXINC')]      = './sql/inception_date_isg.sql'

dict_inceptiondt[('GCFUND','CMDTYAGG')]    = './sql/inception_date_isg.sql'

dict_inceptiondt[('GCFUND','FIXINCAGG')]   = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_CIT','FUND')]       = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_CIT','CMDTY')]      = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_CIT','FIXINC')]     = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_CIT','CMDTYAGG')]   = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_CIT','FIXINCAGG')]  = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_MUT','FUND')]       = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_MUT','CMDTY')]      = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_MUT','FIXINC')]     = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_MUT','CMDTYAGG')]   = './sql/inception_date_isg.sql'

dict_inceptiondt[('PRI_MUT','FIXINCAGG')]  = './sql/inception_date_isg.sql'

dict_inceptiondt[('HARBOR','FUND')]       = './sql/inception_date_isg.sql'

dict_inceptiondt[('HARBOR','CMDTY')]      = './sql/inception_date_isg.sql'

dict_inceptiondt[('HARBOR','FIXINC')]     = './sql/inception_date_isg.sql'

dict_inceptiondt[('HARBOR','CMDTYAGG')]   = './sql/inception_date_isg.sql'

dict_inceptiondt[('HARBOR','FIXINCAGG')]  = './sql/inception_date_isg.sql'

 

dict_indexprice = {}

dict_indexprice[('APFUND','OYGLSNE')] = 'APGEXEN'

dict_indexprice[('APFUND','GSCINE')] = 'APGEXEN'

 

dict_fullyfunded = {}

dict_fullyfunded[('INPRS','OYDLS1')] = './sql/fullyfunded_inprs.sql'

 

dict_pnl = {}

dict_pnl[('ABCAP','FARFUT1X')]     = './sql/pnl_abcap_farfut1x.sql'

dict_pnl[('AEGON','AEG_TOP')]      = './sql/pnl_aegon_top.sql'

dict_pnl[('AEGON','AEG_FX')]       = './sql/pnl_aegon_aeg_fx.sql'

dict_pnl[('DBSEL','FARFUT125X')]   = './sql/pnl_abcap_farfut1x.sql'

dict_pnl[('FARFUND','FAR')]        = './sql/pnl_far.sql'

dict_pnl[('GALAXY','FAR')]         = './sql/pnl_far.sql'

dict_pnl[('JCP','FAR')]            = './sql/pnl_far.sql'

dict_pnl[('FARFUND','FARRM')]      = './sql/pnl_farrm.sql'

dict_pnl[('GALAXY','FARRM')]       = './sql/pnl_farrm.sql'

dict_pnl[('JCP','FARRM')]          = './sql/pnl_farrm.sql'

dict_pnl[('NEST','OYDLSESG')]      = './sql/pnl_oydlsesg.sql'

dict_pnl[('JCP','OYDLSEQ')]        = './sql/pnl_oydlseq.sql'

dict_pnl[('JIS_CSAF','FUND')]      = './sql/pnl_isg_fund.sql'

dict_pnl[('JIS_CSAF','CMDTY')]     = './sql/pnl_isg_cmdty.sql'

dict_pnl[('JIS_CSAF','EQTY')]      = './sql/pnl_isg_equity.sql'

dict_pnl[('JIS_CSAF','FIXINC')]    = './sql/pnl_isg_tips.sql'

dict_pnl[('JIS_CSAF','CMDTYAGG')]  = './sql/pnl_isg_cmdty.sql'

dict_pnl[('JIS_CSAF','EQTYAGG')]   = './sql/pnl_isg_equity.sql'

dict_pnl[('JIS_CSAF','FIXINCAGG')] = './sql/pnl_isg_tips.sql'

dict_pnl[('CITFUND','FUND')]       = './sql/pnl_isg_fund.sql'

dict_pnl[('CITFUND','CMDTY')]      = './sql/pnl_isg_cmdty.sql'

dict_pnl[('CITFUND','EQTY')]       = './sql/pnl_isg_equity.sql'

dict_pnl[('CITFUND','FIXINC')]     = './sql/pnl_isg_tips.sql'

dict_pnl[('CITFUND','CMDTYAGG')]   = './sql/pnl_isg_cmdty.sql'

dict_pnl[('CITFUND','EQTYAGG')]    = './sql/pnl_isg_equity.sql'

dict_pnl[('CITFUND','FIXINCAGG')]  = './sql/pnl_isg_tips.sql'

dict_pnl[('GCFUND','FUND')]        = './sql/pnl_isg_fund.sql'

dict_pnl[('GCFUND','CMDTY')]       = './sql/pnl_isg_cmdty.sql'

dict_pnl[('GCFUND','CMDTYAGG')]    = './sql/pnl_isg_cmdty.sql'

dict_pnl[('GCFUND','FIXINC')]      = './sql/pnl_isg_tips.sql'

dict_pnl[('GCFUND','FIXINCAGG')]   = './sql/pnl_isg_tips.sql'

dict_pnl[('PRI_CIT','FUND')]       = './sql/pnl_isg_fund.sql'

dict_pnl[('PRI_CIT','CMDTY')]      = './sql/pnl_isg_cmdty.sql'

dict_pnl[('PRI_CIT','CMDTYAGG')]   = './sql/pnl_isg_cmdty.sql'

dict_pnl[('PRI_CIT','FIXINC')]     = './sql/pnl_isg_tips.sql'

dict_pnl[('PRI_CIT','FIXINCAGG')]  = './sql/pnl_isg_tips.sql'

dict_pnl[('PRI_MUT','FUND')]       = './sql/pnl_isg_fund.sql'

dict_pnl[('PRI_MUT','CMDTY')]      = './sql/pnl_isg_cmdty.sql'

dict_pnl[('PRI_MUT','CMDTYAGG')]   = './sql/pnl_isg_cmdty.sql'

dict_pnl[('PRI_MUT','FIXINC')]     = './sql/pnl_isg_tips.sql'

dict_pnl[('PRI_MUT','FIXINCAGG')]  = './sql/pnl_isg_tips.sql'

dict_pnl[('HARBOR','FUND')]       = './sql/pnl_isg_fund.sql'

dict_pnl[('HARBOR','CMDTY')]      = './sql/pnl_isg_cmdty.sql'

dict_pnl[('HARBOR','CMDTYAGG')]   = './sql/pnl_isg_cmdty.sql'

dict_pnl[('HARBOR','FIXINC')]     = './sql/pnl_isg_tips.sql'

dict_pnl[('HARBOR','FIXINCAGG')]  = './sql/pnl_isg_tips.sql'

 

dict_notional = {}

dict_notional[('JIS_CSAF','FUND')]      = './sql/notional_isg_fund.sql'

dict_notional[('JIS_CSAF','CMDTY')]     = './sql/notional_isg_fund.sql'

dict_notional[('JIS_CSAF','EQTY')]      = './sql/notional_isg_fund.sql'

dict_notional[('JIS_CSAF','FIXINC')]    = './sql/notional_isg_fund.sql'

dict_notional[('JIS_CSAF','CMDTYAGG')]  = './sql/notional_isg_cmdtyagg.sql'

dict_notional[('JIS_CSAF','EQTYAGG')]   = './sql/notional_isg_eqtyagg.sql'

dict_notional[('JIS_CSAF','FIXINCAGG')] = './sql/notional_isg_tipsagg.sql'

dict_notional[('CITFUND','FUND')]       = './sql/notional_isg_fund.sql'

dict_notional[('CITFUND','CMDTY')]      = './sql/notional_isg_fund.sql'

dict_notional[('CITFUND','EQTY')]       = './sql/notional_isg_fund.sql'

dict_notional[('CITFUND','FIXINC')]     = './sql/notional_isg_fund.sql'

dict_notional[('CITFUND','CMDTYAGG')]   = './sql/notional_isg_cmdtyagg.sql'

dict_notional[('CITFUND','EQTYAGG')]    = './sql/notional_isg_eqtyagg.sql'

dict_notional[('CITFUND','FIXINCAGG')]  = './sql/notional_isg_tipsagg.sql'

dict_notional[('CITFUND','FUND')]       = './sql/notional_isg_fund.sql'

dict_notional[('GCFUND','FUND')]        = './sql/notional_isg_fund.sql'

dict_notional[('GCFUND','CMDTY')]       = './sql/notional_isg_fund.sql'

dict_notional[('GCFUND','CMDTYAGG')]    = './sql/notional_isg_cmdtyagg.sql'

dict_notional[('GCFUND','FIXINC')]      = './sql/notional_isg_fund.sql'

dict_notional[('GCFUND','FIXINCAGG')]   = './sql/notional_isg_tipsagg.sql'

dict_notional[('PRI_CIT','FUND')]       = './sql/notional_isg_fund.sql'

dict_notional[('PRI_CIT','CMDTY')]      = './sql/notional_isg_fund.sql'

dict_notional[('PRI_CIT','CMDTYAGG')]   = './sql/notional_isg_cmdtyagg.sql'

dict_notional[('PRI_CIT','FIXINC')]     = './sql/notional_isg_fund.sql'

dict_notional[('PRI_CIT','FIXINCAGG')]  = './sql/notional_isg_tipsagg.sql'

dict_notional[('PRI_MUT','FUND')]       = './sql/notional_isg_fund.sql'

dict_notional[('PRI_MUT','CMDTY')]      = './sql/notional_isg_fund.sql'

dict_notional[('PRI_MUT','CMDTYAGG')]   = './sql/notional_isg_cmdtyagg.sql'

dict_notional[('PRI_MUT','FIXINC')]     = './sql/notional_isg_fund.sql'

dict_notional[('PRI_MUT','FIXINCAGG')]  = './sql/notional_isg_tipsagg.sql'

dict_notional[('HARBOR','FUND')]       = './sql/notional_isg_fund.sql'

dict_notional[('HARBOR','CMDTY')]      = './sql/notional_isg_fund.sql'

dict_notional[('HARBOR','CMDTYAGG')]   = './sql/notional_isg_cmdtyagg.sql'

dict_notional[('HARBOR','FIXINC')]     = './sql/notional_isg_fund.sql'

dict_notional[('HARBOR','FIXINCAGG')]  = './sql/notional_isg_tipsagg.sql'

dict_notional[('FUT1','CUSTOM1')]      = './sql/notional_custom1.sql'

dict_notional[('FUT2','CUSTOM1')]      = './sql/notional_custom1.sql'

dict_notional[('CCNR','FEQ')]          = './sql/notional_ccnr.sql'

 

dict_adjustments = {}

dict_adjustments[('ABCAP','FARFUT1X')]  = './sql/adjustments_abcap_farfut1x.sql'

dict_adjustments[('AEGON','AEG_TOP')]   = './sql/adjustments_aegon_top.sql'

dict_adjustments[('DBSEL','FARFUT125X')]  = './sql/adjustments_abcap_farfut1x.sql'

dict_adjustments[('FARFUND','FAR')]     = './sql/adjustments_far.sql'

dict_adjustments[('GALAXY','FAR')]      = './sql/adjustments_far.sql'

dict_adjustments[('JCP','FAR')]         = './sql/adjustments_far.sql'

dict_adjustments[('FARFUND','FARRM')]   = './sql/adjustments_farrm.sql'

dict_adjustments[('GALAXY','FARRM')]    = './sql/adjustments_farrm.sql'

dict_adjustments[('JCP','FARRM')]       = './sql/adjustments_farrm.sql'

dict_adjustments[('NEST','OYDLSESG')]   = './sql/adjustments_oydlsesg.sql'

dict_adjustments[('JCP','OYDLSEQ')]     = './sql/adjustments_oydlseq.sql'

 

dict_commission = {}

dict_commission[('ABCAP','FARFUT1X')]   = './sql/commission_abcap_farfut1x.sql'

dict_commission[('AEGON','AEG_TOP')]    = './sql/commission_aegon_top.sql'

dict_commission[('DBSEL','FARFUT125X')] = './sql/commission_abcap_farfut1x.sql'

dict_commission[('FARFUND','FAR')]      = './sql/commission_far.sql'

dict_commission[('GALAXY','FAR')]       = './sql/commission_far.sql'

dict_commission[('JCP','FAR')]          = './sql/commission_far.sql'

dict_commission[('FARFUND','FARRM')]    = './sql/commission_farrm.sql'

dict_commission[('GALAXY','FARRM')]     = './sql/commission_farrm.sql'

dict_commission[('JCP','FARRM')]        = './sql/commission_farrm.sql'

dict_commission[('NEST','OYDLSESG')]    = './sql/commission_oydlsesg.sql'

dict_commission[('JCP','OYDLSEQ')]      = './sql/commission_oydlseq.sql'

 

@click.group()

@click.argument('client')

@click.argument('strategy')

@click.option('--asofdate',

              type=click.DateTime(formats=["%Y-%m-%d"]),

              default=str(date.today()),

              help='As Of Date')

@click.pass_context

def main(ctx, client, strategy, asofdate):

    try:

        log.info("")

        log.info("--------------------------------------------------")

        log.info("Running program: " + os.path.basename(__file__))

        log.info("Desc: Calculate Gross Returns")

        log.info("Run Id: " + str(RUNID))

        log.info("Environment: " + os.environ["CORE_ENVIRONMENT"])

        log.info(os.getcwd())

        log.info("--------------------------------------------------")

        log.info("")

 

        f = open(dict_inceptiondt.get((client,strategy), "./sql/get_inception_date.sql"), 'r').read()

        dt_inception = jam.execute(text(f), {'client':client, 'strategy':strategy}).one()[0]

        f = open("./sql/get_monthends.sql", 'r').read()

        datefrom1 = jam.execute(text(open("./sql/get_monthends.1.sql", 'r').read()), {'d':asofdate}).one()[0]

        datefrom2 = jam.execute(text(open("./sql/get_monthends.1.sql", 'r').read()), {'d':datefrom1}).one()[0]

        datefrom = list(map(lambda x:max(dt_inception, x), [datefrom2]))[0]

        #

        log.info("date from: " + str(datefrom))

        log.info("date to: "   + str(asofdate.date()))

        log.info("inception date: "   + str(dt_inception))

        log.info("client: "    + client)

        log.info("strategy: "  + strategy)

        #

        ctx.obj = {

            'asofdate': asofdate.date(),

            'datefrom': datefrom,

            'inceptiondt': dt_inception,

            'client'  : client,

            'strategy': strategy

            }

    except RequestToTerminateException as e:

        log.info("Signal received to terminate...")

    except:

        log.info("Anonymous exceptions raised...")

        log.info(traceback.format_exc())

    else:

        log.info("-")

 

@main.result_callback()

def process_result(result, **kwargs):

    if os.path.isfile('./grossreturns.pid'):

        os.remove('./grossreturns.pid')

 

@main.command('dly-strat')

@click.pass_context

def dlystrat(ctx):

    asofdate = (ctx.obj)['asofdate']

    datefrom = (ctx.obj)['datefrom']

    client = (ctx.obj)['client']

    strategy = (ctx.obj)['strategy']

    log.info("")

    log.info("calculating daily returns (dly-strat)")

    try:

        f = open(dict_pnl.get((client,strategy), "./sql/getpnl.1.sql"), 'r').read()

        df_pnl = pd.read_sql(f, engine, params=[datefrom, asofdate, client, strategy])

        #

        f = open(dict_notional.get((client,strategy), "./sql/getnotional.1.sql"), 'r').read()

        df_notional = pd.read_sql(f, engine, params=[datefrom, asofdate, client, strategy])

        #

        datefrom_y = pd.to_datetime(datefrom, format="%Y-%m-%d").strftime('%y%m%d')

        dateto_y = pd.to_datetime(asofdate, format="%Y-%m-%d").strftime('%y%m%d')

        f = open(dict_commission.get((client,strategy), "./sql/getcommission.1.sql"), 'r').read()

        df_commission = pd.read_sql(f, engine, params=[datefrom_y, dateto_y, client, strategy])

        #

        f = open(dict_fullyfunded.get((client,strategy), "./sql/getclientattr.sql"), 'r').read()

        df_comintincl = pd.read_sql(f, engine, params=[client])

        cominclude = df_comintincl.iloc[0]['cmmsnInclude']

        intinclude = df_comintincl.iloc[0]['interestInclude']

        #

        f = open(dict_adjustments.get((client,strategy), "./sql/getadjustments.1.sql"), 'r').read()

        df_pnladj = pd.read_sql(f, engine, params=[datefrom, asofdate, 'PNL', client, strategy])

        df_comadj = pd.read_sql(f, engine, params=[datefrom, asofdate, 'COM', client, strategy])

        df_intadj = pd.read_sql(f, engine, params=[datefrom, asofdate, 'INT', client, strategy])

        #

        df_pnladj.rename(columns={'adjust_dly':'adjpnl'}, inplace=True)

        df_comadj.rename(columns={'adjust_dly':'adjcom'}, inplace=True)

        df_intadj.rename(columns={'adjust_dly':'adjint'}, inplace=True)

        df_pnl.set_index("asofdate", inplace = True)

        df_notional.set_index("asofdate", inplace = True)

        df_commission.set_index("asofdate", inplace = True)

        df_pnladj.set_index("asofdate", inplace = True)

        df_comadj.set_index("asofdate", inplace = True)

        df_intadj.set_index("asofdate", inplace = True)

        #

        df_intadj = df_intadj * intinclude

        df_comadj = df_comadj * cominclude

        df_notional = df_notional.shift(1)

        #

        frames = [df_pnl, df_notional, df_commission, df_pnladj, df_comadj, df_intadj]

        df1 = reduce(lambda l,r: pd.merge(l,r, how='outer', on=['asofdate']), frames).fillna(0)

        df1 = df1.sort_values(by=['asofdate'])

        df1 = df1[1:]

        df1['netpnl'] = df1['pnl']-df1['commission']+df1['adjpnl']+df1['adjcom']+df1['adjint']

        df1['grossret'] = df1['netpnl']/df1['notional']

        df1.reset_index(inplace=True)

        #

        oRefId = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'clientstrat',

            c_ref_fnc_identity_gross.refrollup == 'dly',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        df1.apply(lambda x: jam.add(c_staging1().initialize(oRefId.id, x)), axis=1)

        jam.commit()

 

        if client in lst_nobenchmark:

            log.info("-SUCCESS-")

            return

    except NoResultFound:

        log.info("sqlalchemy.exc.NoResultFound. No row was found. check identity table setup.")

        sys.exit(100)

    except:

        log.info("Anonymous exceptions raised...")

        jam.rollback()

        log.info(traceback.format_exc())

    else:

        log.info("-SUCCESS-")

    finally:

        if os.path.isfile('./grossreturns.pid'):

            os.remove('./grossreturns.pid')

 

@main.command('dly-bmark')

@click.pass_context

def dlybmark(ctx):

    asofdate = (ctx.obj)['asofdate']

    datefrom = (ctx.obj)['datefrom']

    client = (ctx.obj)['client']

    strategy = (ctx.obj)['strategy']

    log.info("")

    log.info("calculating daily bmark returns (dly-bmark)")

    try:

        if client in lst_nobenchmark:

            log.info("-SUCCESS-")

            return

        f = open("./sql/getstratbmark.sql", 'r').read()

        df_benchmark = pd.read_sql(f, engine, params=[client, strategy])

        benchmark1 = df_benchmark.iloc[0]['benchmark']

        benchmark  = dict_indexprice.get((client, strategy), benchmark1)

        #

        f = open("./sql/get_index_prices.sql", 'r').read()

        df_indexprices = pd.read_sql(f, engine, params=[benchmark, datefrom, asofdate])

        df_indexprices.set_index("asofdate", inplace = True)

        df_indexprices['grossret'] = df_indexprices.pct_change()

        df_indexprices = df_indexprices[1:]

        df_indexprices.reset_index(inplace = True)

        assert df_indexprices.empty == False, "missing index prices. check index name."

        #

        obmarkid = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'bmark',

            c_ref_fnc_identity_gross.refrollup == 'dly',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        df_indexprices.apply(lambda x: jam.add(c_staging1().initialize(obmarkid.id, x)), axis=1)

        jam.commit()

        

    except NoResultFound:

        log.info("sqlalchemy.exc.NoResultFound. No row was found. check identity table setup.")

        sys.exit(100)

    except:

        log.info("Anonymous exceptions raised...")

        jam.rollback()

        log.info(traceback.format_exc())

    else:

        log.info("-SUCCESS-")

    finally:

        if os.path.isfile('./grossreturns.pid'):

            os.remove('./grossreturns.pid')

 

@main.command('mqy-strat')

@click.pass_context

def mqybmark(ctx):

    asofdate = (ctx.obj)['asofdate']

    datefrom = (ctx.obj)['datefrom']

    client = (ctx.obj)['client']

    strategy = (ctx.obj)['strategy']

    inceptiondt = (ctx.obj)['inceptiondt']

    log.info("")

    log.info("calculating m/q/y/i(td) returns (mqy-strat)")

    try:

        f = open("./sql/get_yearends.sql", 'r').read()

        df_date_period = pd.read_sql(f, engine, params=[asofdate])

        dt_lastyearend1 = df_date_period['asofdate'][0]

        dt_lastyearend = list(map(lambda x:max(inceptiondt, x), [dt_lastyearend1]))[0]

        #

        oRefId = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'clientstrat',

            c_ref_fnc_identity_gross.refrollup == 'dly',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        f = open("./sql/get_dailygross.sql", 'r').read()

        df_dlygross = pd.read_sql(f, engine, params=[dt_lastyearend, asofdate, oRefId.id])

        assert df_dlygross.empty == False, "daily returns missing"

        #

        ostratitd = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'clientstrat',

            c_ref_fnc_identity_gross.refrollup == 'itd',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        df_returns = df_dlygross

        idt_yearend_flag = False

        if inceptiondt != dt_lastyearend:

            f = open("./sql/get_itdgross.sql", 'r').read()

            df_itdgross = pd.read_sql(f, engine, params=[dt_lastyearend, ostratitd.id])

            assert df_itdgross.empty == False, "itd anchoring data point missing"

            frames = [df_itdgross, df_dlygross]

            df_returns = pd.concat([df_itdgross, df_dlygross])

            idt_yearend_flag = True

        #

        df_returns['grossdly1'] = df_returns['grossdly']+1

        df_returns['ofmonth'] = pd.DatetimeIndex(df_returns['asofdate']).month

        df_returns['ofyear'] = pd.DatetimeIndex(df_returns['asofdate']).year

        df_returns['ofqtr'] = pd.DatetimeIndex(df_returns['asofdate']).quarter

        df_returns['itd'] = df_returns['grossdly1'].cumprod()-1

        if idt_yearend_flag:

            df_returns = df_returns[1:]

        df_returns['mtd'] = df_returns.groupby(['ofmonth'])['grossdly1'].cumprod()-1

        df_returns['qtd'] = df_returns.groupby(['ofqtr'])['grossdly1'].cumprod()-1

        df_returns['ytd'] = df_returns.groupby(['ofyear'])['grossdly1'].cumprod()-1

        df_returns['asofdate'] = pd.to_datetime(df_returns['asofdate'])

        #dtfrom = datetime(datefrom.year, datefrom.month, datefrom.day)

        #dtto = datetime(asofdate.year, asofdate.month, asofdate.day)

        #df1 = df_returns.loc[(df_returns['asofdate'] > dtfrom) & (df_returns['asofdate'] <= dtto)]

        df1 = df_returns

        #

        ostratytd = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'clientstrat',

            c_ref_fnc_identity_gross.refrollup == 'ytd',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        ostratqtd = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'clientstrat',

            c_ref_fnc_identity_gross.refrollup == 'qtd',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        ostratmtd = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'clientstrat',

            c_ref_fnc_identity_gross.refrollup == 'mtd',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        df1.apply(lambda x: jam.add(c_staging1().init(ostratitd.id, x['asofdate'], x['itd'])), axis=1)

        df1.apply(lambda x: jam.add(c_staging1().init(ostratytd.id, x['asofdate'], x['ytd'])), axis=1)

        df1.apply(lambda x: jam.add(c_staging1().init(ostratqtd.id, x['asofdate'], x['qtd'])), axis=1)

        df1.apply(lambda x: jam.add(c_staging1().init(ostratmtd.id, x['asofdate'], x['mtd'])), axis=1)

        jam.commit()



    except:

        log.info("Anonymous exceptions raised...")

        jam.rollback()

        log.info(traceback.format_exc())

    else:

        log.info("-SUCCESS-")

    finally:

        if os.path.isfile('./grossreturns.pid'):

            os.remove('./grossreturns.pid')

 

@main.command('mqy-bmark')

@click.pass_context

def mqybmark(ctx):

    asofdate = (ctx.obj)['asofdate']

    datefrom = (ctx.obj)['datefrom']

    client = (ctx.obj)['client']

    strategy = (ctx.obj)['strategy']

    inceptiondt = (ctx.obj)['inceptiondt']

    log.info("")

    log.info("calculating m/q/y/i(td) returns (mqy-bmark)")

    try:

        #f = open("./sql/get_yearends.sql", 'r').read()

        f = open("./sql/getrolling12mnths.sql", 'r').read()

        df_date_period = pd.read_sql(f, engine, params=[asofdate])

        #dt_start1 = df_date_period['asofdate'][0]

        dt_start1 = df_date_period.iloc[-1]['asofdate']

        dt_start = list(map(lambda x:max(inceptiondt, x), [dt_start1]))[0]

        #

        if client in lst_nobenchmark:

            log.info("no benchmark associated with this client")

            log.info("-SUCCESS-")

            return

 

        # benchmark

        f = open("./sql/getstratbmark.sql", 'r').read()

        df_benchmark = pd.read_sql(f, engine, params=[client, strategy])

        benchmark1 = df_benchmark.iloc[0]['benchmark']

        benchmark  = dict_indexprice.get((client, strategy), benchmark1)

        #

        f1 = open("./sql/get_index_prices.2.sql", 'r').read()

        f2 = open("./sql/get_index_prices.3.sql", 'r').read()

        f3 = open("./sql/get_index_prices.4.sql", 'r').read()

        f4 = open("./sql/get_index_prices.sql", 'r').read()

        f5 = open("./sql/get_index_prices.sql", 'r').read()

        df_mly_idxpx1 = pd.read_sql(f1, engine, params=[benchmark, dt_start, asofdate])

        df_qly_idxpx1 = pd.read_sql(f2, engine, params=[benchmark, dt_start, asofdate])

        df_yly_idxpx1 = pd.read_sql(f3, engine, params=[benchmark, dt_start, asofdate])

        df_inc_idxpx1 = pd.read_sql(f4, engine, params=[benchmark, inceptiondt, inceptiondt])

        df_indexprices = pd.read_sql(f5, engine, params=[benchmark, dt_start, asofdate])

        #df_mly_idxpx1 = pd.read_sql(f1, engine, params=[benchmark, inceptiondt, asofdate])

        #df_qly_idxpx1 = pd.read_sql(f2, engine, params=[benchmark, inceptiondt, asofdate])

        #df_yly_idxpx1 = pd.read_sql(f3, engine, params=[benchmark, inceptiondt, asofdate])

        #df_inc_idxpx1 = pd.read_sql(f4, engine, params=[benchmark, inceptiondt, inceptiondt])

        #df_indexprices = pd.read_sql(f5, engine, params=[benchmark, inceptiondt, asofdate])

        df_mly_idxpx1.set_index("asofdate", inplace = True)

        df_qly_idxpx1.set_index("asofdate", inplace = True)

        df_yly_idxpx1.set_index("asofdate", inplace = True)

        df_inc_idxpx1.set_index("asofdate", inplace = True)

        df_indexprices.set_index("asofdate", inplace = True)

        df_mly_idxpx1.rename(columns={'price':'mly'}, inplace=True)

        df_qly_idxpx1.rename(columns={'price':'qly'}, inplace=True)

        df_yly_idxpx1.rename(columns={'price':'yly'}, inplace=True)

        df_inc_idxpx1.rename(columns={'price':'inc'}, inplace=True)

        frames = [df_indexprices, df_mly_idxpx1, df_qly_idxpx1, df_yly_idxpx1]

        df3 = reduce(lambda l,r: pd.merge(l,r, how='left', on=['asofdate']), frames) #.fillna(0)

        df4 = pd.merge(df_inc_idxpx1, df3, how='outer', on=['asofdate'])

        df5 = df4.ffill(axis = 0)

        mask = ~(df5.columns.isin(['business_day', 'price']))

        cols_to_shift = df5.columns[mask]

        df5[cols_to_shift] = df5[cols_to_shift].shift(1)

        df6 = df5[1:]

        df7 = pd.DataFrame()

        df7['mtd'] = df6.loc[:, ['mly','price']].pct_change(axis=1)['price']

        df7['qtd'] = df6[['qly','price']].pct_change(axis=1)['price']

        df7['ytd'] = df6[['yly','price']].pct_change(axis=1)['price']

        df7['itd'] = df6[['inc','price']].pct_change(axis=1)['price']

        df7.reset_index(inplace=True)

        df7['asofdate'] = pd.to_datetime(df7['asofdate'])

        datefrom1 = datetime(datefrom.year, datefrom.month, datefrom.day)

        df7 = df7.loc[df7['asofdate'] > datefrom1]

        #

        obmrkitd = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'bmark',

            c_ref_fnc_identity_gross.refrollup == 'itd',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        obmrkytd = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'bmark',

            c_ref_fnc_identity_gross.refrollup == 'ytd',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        obmrkqtd = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'bmark',

            c_ref_fnc_identity_gross.refrollup == 'qtd',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        obmrkmtd = jam.query(c_ref_fnc_identity_gross).filter(

            c_ref_fnc_identity_gross.clients == client,

            c_ref_fnc_identity_gross.strategy == strategy,

            c_ref_fnc_identity_gross.reflevel == 'bmark',

            c_ref_fnc_identity_gross.refrollup == 'mtd',

            c_ref_fnc_identity_gross.type == 'gross'

            ).one()

        df7.apply(lambda x: jam.add(c_staging1().init(obmrkitd.id, x['asofdate'], x['itd'])), axis=1)

        df7.apply(lambda x: jam.add(c_staging1().init(obmrkytd.id, x['asofdate'], x['ytd'])), axis=1)

        df7.apply(lambda x: jam.add(c_staging1().init(obmrkqtd.id, x['asofdate'], x['qtd'])), axis=1)

        df7.apply(lambda x: jam.add(c_staging1().init(obmrkmtd.id, x['asofdate'], x['mtd'])), axis=1)

        jam.commit()

    except:

        log.info("Anonymous exceptions raised...")

        jam.rollback()

        log.info(traceback.format_exc())

    else:

        log.info("-SUCCESS-")

    finally:

        if os.path.isfile('./grossreturns.pid'):

            os.remove('./grossreturns.pid')

 

@main.command('update-legacy')

@click.pass_context

def mqybmark(ctx):

    asofdate = (ctx.obj)['asofdate']

    datefrom = (ctx.obj)['datefrom']

    log.info("")

    log.info("updating legacy tables")

    try:

        f = open("./sql/getbusinessdays.sql", 'r').read()

        df_busdates = pd.read_sql(f, engine, params=[datefrom, asofdate])

        for index, row in df_busdates.iterrows():

            log.info("updating legacy table for date: " + str(row['asofdate']))

            x = jam.execute(open("./sql/stage_strat.sql", 'r').read(), {'d':row['asofdate']})

            y = jam.execute(open("./sql/stage_bmark.sql", 'r').read(), {'d':row['asofdate']})

            z = jam.execute(open("./sql/stage_alpha_outperf.sql", 'r').read(), {'d':row['asofdate']})

        jam.commit()

        log.info("-SUCCESS-")

    except:

        jam.rollback()

        log.info(traceback.format_exc())

 

@main.command('true-up')

@click.pass_context

def trueup(ctx):

    asofdate = (ctx.obj)['asofdate']

    datefrom = (ctx.obj)['datefrom']

    client = (ctx.obj)['client']

    log.info("")

    log.info("true-up returns to the fund level")

    try:

        f = open("./sql/grossreturns.1.sql", 'r').read()

        df1a = pd.read_sql(text(f), engine, params={'fromdate':datefrom, 'todate':asofdate, 'client':client})

        #

        periods = ['dly', 'mtd', 'qtd', 'ytd', 'itd']

        for period in periods:

            df1b = df1a.loc[df1a['refrollup'] == period]

            df1 = df1b.drop(columns=['refrollup'])

            df2 = pd.pivot_table(df1, values='val', index=['asofdate'], columns='strategy')

            df3 = pd.DataFrame(df2.to_records())

            df3['asofdate'] = pd.to_datetime(df3['asofdate'])

            df3.set_index("asofdate", inplace=True)

            df3.sort_values(by=['asofdate'], inplace=True)

            c = df3.columns[~df3.columns.isin(['FUND'])]

            df4 = df3[c]

            df5 = df3[['FUND']]

            #

            x = df5.sub(df4.sum(axis=1), axis=0)

            y = df4.abs().divide(df4.abs().sum(axis=1), axis=0)

            z = y.mul(x['FUND'], axis=0)

            df6 = df4 + z

            #

            assets = df6.columns.tolist()

            df6.reset_index(inplace=True)

            for asset in assets:

                o = jam.query(c_ref_fnc_identity_gross).filter(

                    c_ref_fnc_identity_gross.clients == client,

                    c_ref_fnc_identity_gross.strategy == asset,

                    c_ref_fnc_identity_gross.reflevel == 'clientstrat',

                    c_ref_fnc_identity_gross.refrollup == period,

                    c_ref_fnc_identity_gross.type == 'gross'

                    ).one()

                df6.apply(lambda x: jam.add(c_staging1().init(o.id, x['asofdate'], x[asset])), axis=1)

                jam.commit()

        jam.commit()

        log.info("-SUCCESS-")

    except:

        jam.rollback()

        log.info(traceback.format_exc())

 

@main.command('clean')

def clean():

    try:

        log.info("cleaning up staging table")

        rowsdeleted = jam.query(c_staging1).delete()

        jam.commit()

        log.info("removed " + str(rowsdeleted) + " rows from staging table")

        log.info("-SUCCESS-")

    except:

        jam.rollback()

        log.info(traceback.format_exc())

 

@main.command('merge')

def merge():

    try:

        log.info("merging staging table into master")

        r = jam.execute(open("./sql/staging_to_main.sql", 'r').read(), {})

        jam.commit()

        log.info("-SUCCESS-")

    except:

        jam.rollback()

        log.info(traceback.format_exc())

 

if __name__ == '__main__':

    main()