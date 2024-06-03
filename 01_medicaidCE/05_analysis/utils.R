# -------------------------------------
# Script: Utilities
# Author: Rachael Ross (copied from https://github.com/kathoffman/lida-comprisks/blob/main/R/utils.R, 
#       written by Nima Hejazi for Kat Hoffman)
# Purpose: Functions for simultaneous CIs and istonic regression (for monotonicity of survival curves)
# Notes:
# -------------------------------------

cb_simult <- function(eif, ci_level) {
  #############################################################################
  # computes the multiplier for a simultaneous confidence band at a given level
  # output: scalar multiplier for creating a simultaneous confidence band
  # input: estimated EIF matrix and nominal confidence level for the band
  #############################################################################
  vcov_eif <- stats::cov(eif)
  rho_eif <- vcov_eif / sqrt(tcrossprod(diag(vcov_eif)))
  mvtnorm_eif <- mvtnorm::qmvnorm(ci_level, tail = "both", corr = rho_eif)
  ci_scaling <- abs(mvtnorm_eif$quantile)
  return(ci_scaling)
}


isoproj <- function(est, eif, ci_level, ci_type = c("marginal", "simult")) {
  #############################################################################
  # projects survival estimates via isotonic regression, enforcing monotonicity
  # output: table of monotonic survival estimates across several timepoints
  # input: tables of survival estimates and corresponding EIFs at timepoints
  #############################################################################
  ci_type <- match.arg(ci_type)

  # compute CI multiplier constant
  if (ci_type == "marginal") {
    ci_mult <- abs(qnorm(p = (1 - ci_level) / 2))
  } else if (ci_type == "simult") {
    ci_mult <- cb_simult(eif, ci_level)
  }

  # projection by isotonic regression
  surv_isoproj <- isotone::gpava(z = est$time, y = est$est)
  est$est <- surv_est_iso <- surv_isoproj$x

  # construct CIs around corrected point estimates
  est$std_err <- se_eif <- sqrt(matrixStats::colVars(eif) / nrow(eif))

  # reconstruct CIs afer isotonicity correction
  # NOTE: Westling et al. claim (right after their Corollary 1, p3041) that
  # "Theorem 2 implies that Wald-type confidence bands constructed around [the
  # original estimate] have the same asymptotic coverage if they are
  # constructed around [the corrected estimate] instead."
  if (ci_type == "marginal") {
    # standard confidence limits can just be symmetric around isotonic
    # projection-corrected point estimates
    est$ci_lwr <- surv_est_iso_cil <- surv_est_iso - ci_mult * se_eif
    est$ci_upr <- surv_est_iso_ciu <- surv_est_iso + ci_mult * se_eif
  } else if (ci_type == "simult") {
    # isotonic projection of lower limit of simultaneous confidence band
    surv_cil_isoproj <- isotone::gpava(
      z = est$time, y = 1 - (surv_est_iso - ci_mult * se_eif)
    )
    est$ci_lwr <- surv_est_iso_cil <- 1 - surv_cil_isoproj$y

    # isotonic projection of upper limit of simultaneous confidence band
    surv_ciu_isoproj <- isotone::gpava(
      z = est$time, y = 1 - (surv_est_iso + ci_mult * se_eif)
    )
    est$ci_upr <- surv_est_iso_ciu <- 1 - surv_ciu_isoproj$y
  }
  data.table::setattr(est, "ci_type", ci_type)
  return(est)
}
